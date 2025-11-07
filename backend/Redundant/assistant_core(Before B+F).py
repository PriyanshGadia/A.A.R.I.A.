# assistant_core_fixed.py
"""
Assistant Core - Fixed Version with Robust Error Handling
"""

import os
import asyncio
import logging
from datetime import datetime, UTC, timedelta
from typing import List, Dict, Any, Optional
from uuid import uuid4
from secure_store import SecureStorageAsync

# Configure logging
logger = logging.getLogger(__name__)

def parse_iso_datetime(dt_str: str) -> datetime:
    """Safely parse ISO 8601 datetime with UTC timezone awareness."""
    try:
        normalized = dt_str.replace('Z', '+00:00')
        dt = datetime.fromisoformat(normalized)
        return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
    except Exception:
        return datetime.now(UTC)

def generate_id(prefix: str = "item") -> str:
    """Generate unique, sortable ID."""
    timestamp = int(datetime.now(UTC).timestamp() * 1000)
    random_suffix = uuid4().hex[:8]
    return f"{prefix}_{timestamp}_{random_suffix}"

class AssistantCore:
    """
    Production-grade AI Assistant Core with robust error handling.
    """

    def __init__(self, password: str, storage_path: str = None, auto_recover: bool = True):
        self.password = password
        self.storage_path = storage_path
        self.auto_recover = auto_recover
        self.store: Optional[SecureStorageAsync] = None
        self._is_initialized = False

    async def initialize(self):
        """Initialize the core with comprehensive error recovery."""
        if self._is_initialized:
            return
            
        try:
            # Initialize secure storage
            storage_args = {"db_path": self.storage_path} if self.storage_path else {}
            self.store = SecureStorageAsync(**storage_args)
            await self.store.connect()

            # Handle master key setup
            if not self.store.has_wrapped_master():
                logger.info("Creating new master key...")
                self.store.create_and_store_master_key(self.password)
                logger.info("✅ New master key created successfully")
            else:
                logger.info("Unlocking existing master key...")
                self.store.unlock_with_password(self.password)
                logger.info("✅ Master key unlocked successfully")

            # Test data access with recovery
            try:
                profile = await self.load_user_profile()
                logger.info("✅ Successfully accessed existing user profile")
            except Exception as e:
                if "Decryption failed" in str(e) or "incorrect master key" in str(e):
                    logger.error("🔑 Encryption key mismatch detected!")
                    if self.auto_recover:
                        logger.warning("🔄 Attempting automatic recovery...")
                        await self._recover_from_corruption()
                    else:
                        raise RuntimeError(
                            "Encryption key mismatch. Data was encrypted with a different key.\n"
                            "Solution: Delete the existing database files and start fresh, "
                            "or use the correct password that was originally used."
                        ) from e
                else:
                    raise

            # Ensure default profile exists
            if not await self.load_user_profile():
                await self.save_user_profile({
                    "name": "New User", 
                    "timezone": "UTC",
                    "created": datetime.now(UTC).isoformat()
                })
                logger.info("✅ Created default user profile")

            self._is_initialized = True
            logger.info("🎉 AssistantCore initialized successfully")
            
        except Exception as e:
            logger.error(f"❌ Initialization failed: {e}")
            await self.close()
            raise

    async def _recover_from_corruption(self):
        """Recover from corrupted storage by resetting everything."""
        logger.warning("Starting storage recovery process...")
        
        if self.store:
            await self.store.close()
        
        # Delete all storage files
        files_to_remove = [
            self.storage_path or "assistant_store.db",
            "master_key.wrapped",
            f"{self.storage_path or 'assistant_store.db'}-wal",
            f"{self.storage_path or 'assistant_store.db'}-shm"
        ]
        
        removed_count = 0
        for file in files_to_remove:
            if os.path.exists(file):
                try:
                    os.remove(file)
                    removed_count += 1
                    logger.info(f"🗑️  Removed corrupted file: {file}")
                except Exception as e:
                    logger.warning(f"Could not remove {file}: {e}")

        logger.info(f"✅ Removed {removed_count} corrupted files")
        
        # Reinitialize storage
        self.store = SecureStorageAsync(
            db_path=self.storage_path or "assistant_store.db"
        )
        await self.store.connect()
        self.store.create_and_store_master_key(self.password)
        logger.info("🔄 Recreated storage with new encryption keys")

    async def __aenter__(self):
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    # ---------------- Core Methods ----------------

    async def save_user_profile(self, profile: Dict[str, Any]) -> None:
        """
        Save or update user profile.
        FIXED: Added the required 'type_' argument to self.store.put()
        """
        if not isinstance(profile, dict):
            raise ValueError("Profile must be a dictionary")
            
        profile["last_updated"] = datetime.now(UTC).isoformat()
        
        # The 'put' method requires (id, type_, obj).
        # We were missing the 'type_' argument.
        await self.store.put("user_profile", "user_profile_data", profile)

    async def load_user_profile(self) -> Dict[str, Any]:
        """Load user profile with empty dict fallback."""
        return await self.store.get("user_profile") or {}

    async def add_contact(self, contact_data: Dict[str, Any]) -> str:
        """Add a new contact and return generated ID."""
        if "name" not in contact_data:
            raise ValueError("Contact must have a 'name' field")

        contact_id = generate_id("contact")
        now = datetime.now(UTC).isoformat()
        
        contact_data.update({
            "id": contact_id,
            "created": now,
            "last_updated": now
        })

        await self.store.put(contact_id, contact_data)
        return contact_id

    async def add_contacts_batch(self, contacts_list: List[Dict[str, Any]]) -> List[str]:
        """Add multiple contacts efficiently and concurrently."""
        tasks = [self.add_contact(contact) for contact in contacts_list]
        contact_ids = await asyncio.gather(*tasks)
        return list(contact_ids)

    async def list_contacts(self) -> List[Dict[str, Any]]:
        """List all contacts."""
        items = await self.store.list_by_type("contact")
        return [item["obj"] for item in items]

    async def add_event(self, event_data: Dict[str, Any]) -> str:
        """Add a new event with validation."""
        required = ["title", "datetime"]
        for field in required:
            if field not in event_data:
                raise ValueError(f"Event missing required field: {field}")

        event_time = parse_iso_datetime(event_data["datetime"])
        if event_time < datetime.now(UTC):
            raise ValueError("Event datetime cannot be in the past")

        event_id = generate_id("event")
        now = datetime.now(UTC).isoformat()
        
        event_data.update({
            "id": event_id,
            "created": now,
            "last_updated": now
        })

        await self.store.put(event_id, event_data)
        return event_id

    async def list_events(self) -> List[Dict[str, Any]]:
        """List all events."""
        items = await self.store.list_by_type("event")
        return [item["obj"] for item in items]

    async def list_upcoming_events(self, days: int = 30) -> List[Dict[str, Any]]:
        """List upcoming events within specified days."""
        now = datetime.now(UTC)
        cutoff = now + timedelta(days=days)

        events = []
        all_events = await self.list_events()
        for event in all_events:
            try:
                event_time = parse_iso_datetime(event["datetime"])
                if now <= event_time <= cutoff:
                    events.append(event)
            except (KeyError, ValueError):
                continue

        return sorted(events, key=lambda e: parse_iso_datetime(e["datetime"]))

    async def health_check(self) -> Dict[str, Any]:
        """Comprehensive health check."""
        try:
            profile = await self.load_user_profile()
            contacts = await self.list_contacts()
            events = await self.list_events()
            upcoming_events = await self.list_upcoming_events(7)

            return {
                "status": "healthy",
                "core_initialized": self._is_initialized,
                "user_profile": {
                    "exists": bool(profile),
                    "name": profile.get("name", "Not Set")
                },
                "data_counts": {
                    "contacts": len(contacts),
                    "total_events": len(events),
                    "upcoming_7d": len(upcoming_events)
                },
                "timestamp": datetime.now(UTC).isoformat()
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "core_initialized": self._is_initialized,
                "timestamp": datetime.now(UTC).isoformat()
            }

    async def close(self) -> None:
        """Secure async shutdown."""
        if self.store:
            try:
                await self.store.close()
                logger.info("✅ AssistantCore closed successfully")
            except Exception as e:
                logger.warning(f"Error during close: {e}")
        self._is_initialized = False


# --- Fixed Demo Usage ---
async def main():
    """Fixed demonstration that will handle corruption automatically."""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    print("🚀 Starting AssistantCore with Auto-Recovery")
    print("=" * 50)
    
    try:
        # This will automatically recover from corruption
        async with AssistantCore(
            password="secure_password_123", 
            auto_recover=True  # Enable automatic recovery
        ) as assistant:
            
            # Test the system
            health = await assistant.health_check()
            print(f"📊 System Health: {health['status']}")
            print(f"👤 User: {health['user_profile']['name']}")
            print(f"📇 Contacts: {health['data_counts']['contacts']}")
            
            # Add some sample data
            await assistant.save_user_profile({
                "name": "John Doe (Recovered)",
                "email": "john@example.com", 
                "timezone": "UTC",
                "recovered_at": datetime.now(UTC).isoformat()
            })
            
            # Add a contact
            contact_id = await assistant.add_contact({
                "name": "Test Contact",
                "email": "test@example.com"
            })
            print(f"✅ Added contact: {contact_id}")
            
            # Final health check
            health = await assistant.health_check()
            print(f"\n🎉 Final Status: {health['status']}")
            
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        print("\n💡 If this persists, manually delete these files:")
        print("   - assistant_store.db")
        print("   - master_key.wrapped")
        print("   - Then run the application again")

if __name__ == "__main__":
    asyncio.run(main())