import sqlite3
import json
import os
import threading
import time
from typing import Optional, Any

class SimpleStore:
    def __init__(self, db_path: str = "stubs/assistant_store.db"):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._lock = threading.Lock()
        self._init_db()

    def _init_db(self):
        with self._conn() as conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS kv (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at REAL
            );
            """)

    def _conn(self):
        return sqlite3.connect(self.db_path, check_same_thread=False)

    def put(self, *args):
        """
        Support different signatures:
        - put(key, value)
        - put(key, namespace, value)  (namespace is ignored but accepted)
        """
        if len(args) == 2:
            key, value = args
        elif len(args) == 3:
            key, _namespace, value = args
        else:
            raise TypeError("put expects 2 or 3 args")
        value_json = json.dumps(value, default=str, ensure_ascii=False)
        with self._lock:
            with self._conn() as conn:
                conn.execute("INSERT OR REPLACE INTO kv(key, value, updated_at) VALUES (?, ?, ?)", (key, value_json, time.time()))
        return True

    def get(self, key, default=None):
        with self._lock:
            with self._conn() as conn:
                cur = conn.execute("SELECT value FROM kv WHERE key=? LIMIT 1", (key,))
                row = cur.fetchone()
                if not row:
                    return default
                try:
                    return json.loads(row[0])
                except Exception:
                    return row[0]