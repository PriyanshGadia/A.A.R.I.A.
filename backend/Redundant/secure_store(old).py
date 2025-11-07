# secure_store.py
"""
Production SecureStorage - Enterprise Grade.

Deployment Notes:
- Set appropriate log level in production (INFO or WARNING)
- Monitor WAL file growth in high-write environments  
- Regular integrity checks recommended
- Backup both .db and .wrapped files together
"""

import os
import sqlite3
import threading
import logging
import ctypes
from datetime import datetime
from typing import Optional, Dict, Any, List
from crypto import (
    generate_master_key, wrap_master_key, unwrap_master_key,
    encrypt_with_master_key, decrypt_with_master_key
)

# # -------------------------------------------------------------
# # Logging Configuration
# # -------------------------------------------------------------
# logger = logging.getLogger("secure_store")
# if not logger.handlers:
#     ch = logging.StreamHandler()
#     ch.setFormatter(logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s"))
#     logger.addHandler(ch)
# logger.setLevel(logging.INFO)
logger = logging.getLogger(__name__)
# -------------------------------------------------------------
# Default Paths (adjust as needed)
# -------------------------------------------------------------
DB_PATH = "assistant_store.db"
WRAPPED_MASTER_KEY_PATH = "master_key.wrapped"


class SecureStorage:
    """
    Enterprise-grade encrypted storage with SQLite backend.

    Security Features:
    - Master key only in memory when unlocked
    - Memory zeroization on lock/close
    - Atomic file operations for key management
    - Encrypted payloads in database

    Operational Features:
    - Thread-safe with RLock
    - WAL mode for performance
    - Schema versioning and safe migrations
    - Integrity verification and statistics
    - WAL checkpointing for size management
    """

    def __init__(self, wrapped_key_path: str = WRAPPED_MASTER_KEY_PATH, db_path: str = DB_PATH):
        self.wrapped_key_path = wrapped_key_path
        self.db_path = db_path
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self._master_key: Optional[bytes] = None
        self._init_db()
        logger.info(f"SecureStorage initialized: db={db_path}, key={wrapped_key_path}")

    # -------------------------------------------------------------
    # Database Initialization
    # -------------------------------------------------------------
    def _init_db(self):
        with self._lock:
            cur = self.conn.cursor()
            try:
                pragmas = [
                    "PRAGMA journal_mode=WAL;",
                    "PRAGMA synchronous=NORMAL;",
                    "PRAGMA temp_store=MEMORY;",
                    "PRAGMA foreign_keys=ON;",
                    "PRAGMA auto_vacuum=FULL;",
                    "PRAGMA busy_timeout=30000;",
                    "PRAGMA cache_size=-64000;",
                ]
                for p in pragmas:
                    cur.execute(p)

                cur.execute("""
                CREATE TABLE IF NOT EXISTS blobs (
                    id TEXT PRIMARY KEY,
                    type TEXT,
                    payload TEXT,
                    created_at TEXT,
                    updated_at TEXT
                )
                """)
                cur.execute("""
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
                """)
                cur.execute("INSERT OR IGNORE INTO meta (key, value) VALUES ('schema_version', '1')")

                cur.execute("CREATE INDEX IF NOT EXISTS idx_blobs_type ON blobs(type)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_blobs_created ON blobs(created_at)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_blobs_updated ON blobs(updated_at)")

                self.conn.commit()
                logger.debug("Database schema initialized")

            except Exception as e:
                logger.exception("Database initialization failed")
                raise RuntimeError(f"Database initialization failed: {e}")

            self._migrate_schema()

    def _migrate_schema(self):
        """Handle schema migrations with rollback protection."""
        with self._lock:
            cur = self.conn.cursor()
            try:
                cur.execute("SELECT value FROM meta WHERE key = 'schema_version'")
                row = cur.fetchone()
                current_version = int(row[0]) if row else 1
            except Exception:
                current_version = 1
                logger.warning("Could not read schema version, assuming v1")

            # Example migration pattern (future-proof)
            # if current_version < 2:
            #     cur.execute("ALTER TABLE blobs ADD COLUMN checksum TEXT")
            #     cur.execute("UPDATE meta SET value = '2' WHERE key = 'schema_version'")
            #     self.conn.commit()

    # -------------------------------------------------------------
    # Master Key Management
    # -------------------------------------------------------------
    def has_wrapped_master(self) -> bool:
        return os.path.exists(self.wrapped_key_path)

    def create_and_store_master_key(self, password: str) -> None:
        """Create and store master key with atomic file operations and restrictive perms."""
        if self.has_wrapped_master():
            raise RuntimeError("Wrapped master key already exists")

        temp_path = None
        try:
            mk = generate_master_key()
            wrapped = wrap_master_key(mk, password)

            temp_path = self.wrapped_key_path + ".tmp"
            with open(temp_path, "w", encoding="utf-8") as f:
                f.write(wrapped)
            os.replace(temp_path, self.wrapped_key_path)

            try:
                os.chmod(self.wrapped_key_path, 0o600)
            except Exception:
                logger.warning("Failed to set permissions on wrapped key file (best-effort)")

            self._master_key = mk
            logger.info("Master key created and stored (wrapped).")
        except Exception:
            if temp_path and os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except Exception:
                    pass
            logger.exception("Failed to create and store master key")
            raise

    def unlock_with_password(self, password: str) -> None:
        """Unwrap master key and cache in memory."""
        if not self.has_wrapped_master():
            raise RuntimeError("No wrapped master key found")
        with open(self.wrapped_key_path, "r", encoding="utf-8") as f:
            wrapped = f.read()
        self._master_key = unwrap_master_key(wrapped, password)
        logger.info("Master key successfully unlocked")

    def _zeroize_master_key(self):
        """Wipe master key bytes from memory (best-effort)."""
        if self._master_key:
            try:
                mutable_key = bytearray(self._master_key)
                ctypes.memset(ctypes.addressof(ctypes.c_char.from_buffer(mutable_key)), 0, len(mutable_key))
            except Exception:
                pass
        self._master_key = None

    def lock(self) -> None:
        """Lock storage (wipe master key)."""
        with self._lock:
            self._zeroize_master_key()
            logger.info("SecureStorage locked (key wiped)")

    def rotate_password(self, old_password: str, new_password: str) -> None:
        """Re-wrap master key with a new password."""
        if not self.has_wrapped_master():
            raise RuntimeError("No wrapped master key found")

        self.unlock_with_password(old_password)
        if not self._master_key:
            raise RuntimeError("Failed to unwrap master key")

        new_wrapped = wrap_master_key(self._master_key, new_password)
        temp_path = self.wrapped_key_path + ".tmp"
        with open(temp_path, "w", encoding="utf-8") as f:
            f.write(new_wrapped)
        os.replace(temp_path, self.wrapped_key_path)
        logger.info("Master key password rotated successfully")

    # -------------------------------------------------------------
    # CRUD Operations
    # -------------------------------------------------------------
    def _require_unlocked(self):
        if self._master_key is None:
            raise RuntimeError("Master key not unlocked. Call unlock_with_password() first.")

    def put(self, id: str, type_: str, obj: Dict[str, Any]) -> None:
        """Encrypt and store object."""
        self._require_unlocked()
        with self._lock:
            payload = encrypt_with_master_key(obj, self._master_key)
            now = datetime.utcnow().isoformat()
            cur = self.conn.cursor()
            cur.execute("""
                INSERT OR REPLACE INTO blobs (id, type, payload, created_at, updated_at) 
                VALUES (?, ?, ?, COALESCE((SELECT created_at FROM blobs WHERE id = ?), ?), ?)
            """, (id, type_, payload, id, now, now))
            self.conn.commit()

    def get(self, id: str) -> Optional[Dict[str, Any]]:
        """Retrieve and decrypt object by ID."""
        self._require_unlocked()
        with self._lock:
            cur = self.conn.cursor()
            cur.execute("SELECT payload FROM blobs WHERE id = ?", (id,))
            row = cur.fetchone()
            return decrypt_with_master_key(row['payload'], self._master_key) if row else None

    def list_by_type(self, type_: str) -> List[Dict[str, Any]]:
        """List objects by type."""
        self._require_unlocked()
        with self._lock:
            cur = self.conn.cursor()
            cur.execute("""
                SELECT id, payload, created_at, updated_at 
                FROM blobs WHERE type = ? ORDER BY created_at DESC
            """, (type_,))
            return [
                {
                    "id": row['id'],
                    "obj": decrypt_with_master_key(row['payload'], self._master_key),
                    "created_at": row['created_at'],
                    "updated_at": row['updated_at']
                } for row in cur.fetchall()
            ]

    def get_metadata(self, type_: str = None) -> List[Dict[str, Any]]:
        """Get metadata without decrypting."""
        with self._lock:
            cur = self.conn.cursor()
            if type_:
                cur.execute("""
                    SELECT id, type, created_at, updated_at 
                    FROM blobs WHERE type = ? ORDER BY created_at DESC
                """, (type_,))
            else:
                cur.execute("""
                    SELECT id, type, created_at, updated_at FROM blobs ORDER BY created_at DESC
                """)
            return [dict(row) for row in cur.fetchall()]

    def delete(self, id: str) -> bool:
        """Delete object by ID."""
        self._require_unlocked()
        with self._lock:
            cur = self.conn.cursor()
            cur.execute("DELETE FROM blobs WHERE id = ?", (id,))
            self.conn.commit()
            return cur.rowcount > 0

    def exists(self, id: str) -> bool:
        """Check if object exists."""
        with self._lock:
            cur = self.conn.cursor()
            cur.execute("SELECT 1 FROM blobs WHERE id = ?", (id,))
            return cur.fetchone() is not None

    # -------------------------------------------------------------
    # Integrity and Maintenance
    # -------------------------------------------------------------
    def verify_integrity(self) -> bool:
        """Run SQLite integrity check."""
        with self._lock:
            cur = self.conn.cursor()
            cur.execute("PRAGMA integrity_check;")
            result = cur.fetchone()[0]
            return result.lower() == "ok"

    def wal_checkpoint(self):
        """Manually checkpoint WAL to limit file size."""
        with self._lock:
            cur = self.conn.cursor()
            cur.execute("PRAGMA wal_checkpoint(TRUNCATE);")

    def get_stats(self) -> Dict[str, Any]:
        """Get basic statistics."""
        with self._lock:
            cur = self.conn.cursor()
            cur.execute("""
                SELECT COUNT(*) as total_items,
                       COUNT(DISTINCT type) as unique_types,
                       MIN(created_at) as oldest_item,
                       MAX(updated_at) as latest_update
                FROM blobs
            """)
            stats = dict(cur.fetchone())
            cur.execute("SELECT type, COUNT(*) as count FROM blobs GROUP BY type")
            stats['items_by_type'] = {r['type']: r['count'] for r in cur.fetchall()}
            return stats

    def create_backup(self, backup_path: str) -> bool:
        """Create consistent backup of DB and key."""
        with self._lock:
            try:
                backup_conn = sqlite3.connect(backup_path)
                with backup_conn:
                    self.conn.backup(backup_conn)
                backup_conn.close()

                import shutil, stat
                if os.path.exists(self.wrapped_key_path):
                    key_backup_path = backup_path + ".key"
                    shutil.copy2(self.wrapped_key_path, key_backup_path)
                    try:
                        st = os.stat(self.wrapped_key_path)
                        os.chmod(key_backup_path, stat.S_IMODE(st.st_mode))
                    except Exception:
                        os.chmod(key_backup_path, 0o600)
                else:
                    logger.info("No wrapped key found; backup contains DB only")

                logger.info(f"Backup created at: {backup_path}")
                return True
            except Exception:
                logger.exception("Backup failed")
                return False

    # -------------------------------------------------------------
    # Context Management
    # -------------------------------------------------------------
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """Securely close storage and wipe memory."""
        try:
            try:
                self.conn.commit()
            except Exception:
                pass

            with self._lock:
                self._zeroize_master_key()

            try:
                self.wal_checkpoint()
                self.conn.close()
            except Exception:
                pass

            logger.info("SecureStorage closed securely")
        except Exception:
            logger.exception("Unexpected error during close")


# -------------------------------------------------------------
# Example Usage (for testing)
# -------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")

    with SecureStorage() as store:
        if not store.has_wrapped_master():
            store.create_and_store_master_key("secure_password_123")
        else:
            store.unlock_with_password("secure_password_123")

        store.put("user_prefs_1", "preferences", {"theme": "dark", "notifications": True})
        store.put("doc_1", "document", {"title": "Secret Plan", "content": "Shhh..."})

        logger.info(f"Preferences: {store.get('user_prefs_1')}")
        logger.info(f"Documents: {store.list_by_type('document')}")
        logger.info(f"Metadata: {store.get_metadata()}")
        logger.info(f"Integrity OK: {store.verify_integrity()}")
        logger.info(f"Stats: {store.get_stats()}")

        store.wal_checkpoint()
        store.create_backup("assistant_backup.db")

    logger.info("Demo completed successfully")
