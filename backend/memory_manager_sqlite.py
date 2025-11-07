# memory_manager_sqlite.py
# Simple SQLite-based MemoryManager implementing the methods PersonaCore expects.
import sqlite3
import json
import time
import threading
from typing import Optional, List, Dict, Any

DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS memory_index (
    id TEXT PRIMARY KEY,
    subject_id TEXT,
    payload TEXT,
    timestamp REAL
);
CREATE TABLE IF NOT EXISTS transcripts (
    id TEXT PRIMARY KEY,
    memory_id TEXT,
    subject_id TEXT,
    payload TEXT,
    timestamp REAL
);
CREATE TABLE IF NOT EXISTS semantic_index (
    id TEXT PRIMARY KEY,
    memory_id TEXT,
    subject_id TEXT,
    payload TEXT,
    timestamp REAL
);
CREATE TABLE IF NOT EXISTS identities (
    id TEXT PRIMARY KEY,
    payload TEXT,
    timestamp REAL
);
"""

class MemoryManagerSQLite:
    def __init__(self, path: str = "aaria_memory.db", timeout: float = 5.0):
        self.path = path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False, timeout=timeout)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            cur = self._conn.cursor()
            cur.executescript(DB_SCHEMA)
            self._conn.commit()

    # ---------- Index load/save ----------
    def load_index(self) -> Dict[str, Any]:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("SELECT payload FROM memory_index ORDER BY timestamp ASC")
            rows = cur.fetchall()
        mems = []
        for r in rows:
            try:
                mems.append(json.loads(r["payload"]))
            except Exception:
                continue
        return {"memory_index": mems, "count": len(mems), "timestamp": time.time()}

    def save_index(self, data: Dict[str, Any]) -> None:
        # store index as multiple rows (upsert)
        with self._lock:
            cur = self._conn.cursor()
            for m in data.get("memory_index", []):
                payload = json.dumps(m)
                timestamp = m.get("timestamp", time.time())
                cur.execute("INSERT OR REPLACE INTO memory_index(id, subject_id, payload, timestamp) VALUES(?,?,?,?)",
                            (m.get("id"), m.get("subject_id", "owner_primary"), payload, timestamp))
            self._conn.commit()

    # ---------- Memory CRUD ----------
    def append_memory(self, subject_id: str, memory_record: Dict[str, Any]) -> None:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("INSERT OR REPLACE INTO memory_index(id, subject_id, payload, timestamp) VALUES(?,?,?,?)",
                        (memory_record["id"], subject_id, json.dumps(memory_record), memory_record.get("timestamp", time.time())))
            self._conn.commit()

    def put(self, key: str, namespace: str, payload: Dict[str, Any]) -> None:
        # namespace is ignored in this simple impl; we store in memory_index
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("INSERT OR REPLACE INTO memory_index(id, subject_id, payload, timestamp) VALUES(?,?,?,?)",
                        (key, payload.get("subject_id", "owner_primary"), json.dumps(payload), payload.get("timestamp", time.time())))
            self._conn.commit()

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("SELECT payload FROM memory_index WHERE id = ?", (key,))
            r = cur.fetchone()
            if not r:
                return None
            return json.loads(r["payload"])

    # ---------- Transcript & Semantic ----------
    def append_transcript(self, subject_id: str, transcript_entry: Dict[str, Any]) -> None:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("INSERT OR REPLACE INTO transcripts(id, memory_id, subject_id, payload, timestamp) VALUES(?,?,?,?,?)",
                        (transcript_entry["id"], transcript_entry.get("memory_id"), subject_id, json.dumps(transcript_entry), transcript_entry.get("timestamp", time.time())))
            self._conn.commit()

    def put_transcript(self, key: str, transcript_entry: Dict[str, Any]) -> None:
        self.append_transcript(transcript_entry.get("subject_id", "owner_primary"), transcript_entry)

    def put_semantic(self, key: str, semantic_entry: Dict[str, Any]) -> None:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("INSERT OR REPLACE INTO semantic_index(id, memory_id, subject_id, payload, timestamp) VALUES(?,?,?,?,?)",
                        (semantic_entry["id"], semantic_entry.get("memory_id"), semantic_entry.get("subject_id", "owner_primary"), json.dumps(semantic_entry), semantic_entry.get("timestamp", time.time())))
            self._conn.commit()

    def append_semantic(self, subject_id: str, semantic_entry: Dict[str, Any]) -> None:
        self.put_semantic(semantic_entry["id"], semantic_entry)

    def save_transcript_store(self, transcripts: List[Dict[str, Any]]) -> None:
        with self._lock:
            cur = self._conn.cursor()
            for tx in transcripts:
                cur.execute("INSERT OR REPLACE INTO transcripts(id,memory_id,subject_id,payload,timestamp) VALUES(?,?,?,?,?)",
                            (tx.get("id"), tx.get("memory_id"), tx.get("subject_id", "owner_primary"), json.dumps(tx), tx.get("timestamp", time.time())))
            self._conn.commit()

    def save_semantic_index(self, semantic_index: List[Dict[str, Any]]) -> None:
        with self._lock:
            cur = self._conn.cursor()
            for s in semantic_index:
                cur.execute("INSERT OR REPLACE INTO semantic_index(id,memory_id,subject_id,payload,timestamp) VALUES(?,?,?,?,?)",
                            (s.get("id"), s.get("memory_id"), s.get("subject_id", "owner_primary"), json.dumps(s), s.get("timestamp", time.time())))
            self._conn.commit()

    # ---------- Identities ----------
    def get_identity_container(self, identity_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("SELECT payload FROM identities WHERE id = ?", (identity_id,))
            r = cur.fetchone()
            if not r:
                return None
            return json.loads(r["payload"])

    def update_identity_container(self, identity_id: str, container: Dict[str, Any]) -> None:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("INSERT OR REPLACE INTO identities(id, payload, timestamp) VALUES(?,?,?)",
                        (identity_id, json.dumps(container), time.time()))
            self._conn.commit()

    # ---------- Search ----------
    def search_memories(self, subject_id: str, query: str = "", limit: int = 10) -> List[Dict[str, Any]]:
        """Naive fulltext search over stored memory payloads. Returns list of memory dicts."""
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("SELECT payload FROM memory_index WHERE subject_id = ? ORDER BY timestamp DESC LIMIT 1000", (subject_id,))
            rows = cur.fetchall()
        results = []
        q = (query or "").lower()
        for r in rows:
            try:
                payload = json.loads(r["payload"])
            except Exception:
                continue
            txt = (payload.get("user","") + " " + payload.get("assistant","")).lower()
            score = 0.0
            if not q:
                score = 0.1
            else:
                if q in txt:
                    score = 0.6 + min(0.4, txt.count(q) * 0.05)
            if score > 0:
                payload["_search_score"] = score
                results.append(payload)
        results.sort(key=lambda m: m.get("_search_score", 0), reverse=True)
        return results[:limit]

    def persist_important(self, subject_id: str, memory_record: Dict[str, Any]) -> None:
        # alias for append_memory
        self.append_memory(subject_id, memory_record)

    def prune_old_memories(self, max_age_days: int, min_importance: int = 4) -> None:
        cutoff = time.time() - (max_age_days * 24 * 3600)
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("SELECT id, payload FROM memory_index")
            rows = cur.fetchall()
            keep = []
            for r in rows:
                try:
                    p = json.loads(r["payload"])
                except Exception:
                    continue
                ts = p.get("timestamp", 0)
                imp = p.get("importance", 1)
                if ts >= cutoff or imp >= min_importance:
                    keep.append((r["id"], json.dumps(p), ts))
            cur.execute("DELETE FROM memory_index")
            for k, payload, ts in keep:
                cur.execute("INSERT INTO memory_index(id, subject_id, payload, timestamp) VALUES(?,?,?,?)", (k, json.loads(payload).get("subject_id","owner_primary"), payload, ts))
            self._conn.commit()

    # ---------- Utility ----------
    def export_memory_snapshot(self, owner: str = "owner_primary") -> Dict[str, Any]:
        return self.load_index()

    def close(self):
        try:
            self._conn.close()
        except Exception:
            pass