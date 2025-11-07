#!/usr/bin/env python3
"""
clear_memory.py

Backs up assistant_store.db then clears (DELETE FROM ...) all user tables inside it,
vacuuming the DB afterwards.

Usage:
    python clear_memory.py               # will prompt for DB path (default ./assistant_store.db)
    python clear_memory.py /path/to/db   # pass DB path as arg
"""

import os
import shutil
import sqlite3
import sys
from datetime import datetime

def backup_db(db_path: str) -> str:
    if not os.path.isfile(db_path):
        raise FileNotFoundError(f"Database not found at: {db_path}")
    t = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    backup_path = f"{db_path}.bak.{t}"
    shutil.copy2(db_path, backup_path)
    print(f"[+] Backup created: {backup_path}")
    return backup_path

def list_tables(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("SELECT name, type FROM sqlite_master WHERE type IN ('table','view') ORDER BY name;")
    rows = cur.fetchall()
    # Exclude internal sqlite system tables
    filtered = [r[0] for r in rows if not r[0].startswith("sqlite_")]
    return filtered

def clear_all_tables(conn: sqlite3.Connection, tables):
    cur = conn.cursor()
    print("[+] Clearing tables (DELETE FROM ...) ...")
    for t in tables:
        print(f"    - clearing table: {t}")
        cur.execute(f"DELETE FROM \"{t}\";")
    conn.commit()
    # Reclaim space
    print("[+] Running VACUUM to rebuild DB and reclaim space...")
    cur.execute("VACUUM;")
    conn.commit()
    print("[+] VACUUM complete.")

def main():
    if len(sys.argv) > 1:
        db_path = sys.argv[1]
    else:
        db_path = input("Path to assistant SQLite DB (default ./assistant_store.db): ").strip() or "./assistant_store.db"

    if not os.path.isfile(db_path):
        print(f"ERROR: DB not found at: {db_path}")
        sys.exit(2)

    print(f"Database path: {db_path}")
    # Backup first
    try:
        backup_path = backup_db(db_path)
    except Exception as e:
        print(f"ERROR during backup: {e}")
        sys.exit(2)

    # Connect and inspect tables
    conn = sqlite3.connect(db_path)
    try:
        tables = list_tables(conn)
        if not tables:
            print("[!] No user tables found (only sqlite internal tables). Nothing to clear.")
            conn.close()
            sys.exit(0)

        print("\nDetected tables (will be cleared):")
        for t in tables:
            print("  -", t)

        print("\n*** FINAL CONFIRMATION REQUIRED ***")
        print("Type 'CLEAR ALL' (without quotes) to proceed and permanently delete the contents of these tables.")
        print("Type anything else to abort.")
        confirm = input("Confirm: ").strip()
        if confirm != "CLEAR ALL":
            print("Aborted by user. No changes made (backup exists).")
            conn.close()
            sys.exit(0)

        # Perform clear
        clear_all_tables(conn, tables)
        conn.close()
        print("\n[OK] All listed tables were cleared. Backup kept at:", backup_path)
        print("If you want to restore, replace the DB file with the backup or inspect it with sqlite3.")
    except Exception as exc:
        print("ERROR during DB clearing:", exc)
        print("You can restore from the backup at:", backup_path)
        conn.close()
        sys.exit(1)

if __name__ == "__main__":
    main()
