# cleanup_storage.py
import os
import asyncio
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def cleanup_storage():
    """Completely clean up all storage files to fix encryption issues."""
    files_to_remove = [
        "assistant_store.db",
        "master_key.wrapped", 
        "assistant_store.db-wal",
        "assistant_store.db-shm",
        # Backup files that might exist
        "assistant_store.db.backup",
        "master_key.wrapped.backup"
    ]
    
    removed_files = []
    for file in files_to_remove:
        if os.path.exists(file):
            try:
                os.remove(file)
                removed_files.append(file)
                print(f"✅ Removed: {file}")
            except Exception as e:
                print(f"❌ Could not remove {file}: {e}")
    
    if removed_files:
        print(f"\n🎉 Successfully cleaned up {len(removed_files)} files:")
        for file in removed_files:
            print(f"   - {file}")
        print("\n🔄 You can now run your application with fresh storage.")
    else:
        print("ℹ️  No storage files found to clean up.")
    
    return len(removed_files) > 0

if __name__ == "__main__":
    print("🛠️  Storage Cleanup Tool")
    print("=" * 50)
    asyncio.run(cleanup_storage())