"""
Assistant Core - Production AI Assistant Engine
Version: 2.2 - Exception-Safe, UTC-Aware & Optimized

Production Features:
- Full exception safety with graceful degradation
- UTC-aware datetime handling throughout
- Secure encrypted storage backend
- Efficient search and batch operations
- Clean, minimal API surface
"""

import json
from datetime import datetime, UTC, timedelta
from typing import List, Dict, Any, Optional
from uuid import uuid4
from secure_store import SecureStorage


# --- Core Utilities ---

def parse_iso_datetime(dt_str: str) -> datetime:
    """
    Safely parse ISO 8601 datetime with UTC timezone awareness.
    Returns UTC-aware datetime, falls back to current UTC time on error.
    """
    try:
        normalized = dt_str.replace('Z', '+00:00')
        dt = datetime.fromisoformat(normalized)
        return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
    except Exception:
        # Graceful fallback for production resilience
        return datetime.now(UTC)


def generate_id(prefix: str = "item") -> str:
    """
    Generate unique, sortable ID with timestamp and random component.
    Format: prefix_timestamp_random
    """
    timestamp = int(datetime.now(UTC).timestamp() * 1000)
    random_suffix = uuid4().hex[:8]  # 8 chars for readability
    return f"{prefix}_{timestamp}_{random_suffix}"


# --- AI Assistant Core Class ---

class AssistantCore:
    """
    Production-grade AI Assistant Core with secure data management.
    
    Key Features:
    - Exception-safe operations throughout
    - UTC-aware datetime handling
    - Secure encrypted storage
    - Efficient search and batch operations
    - Automatic resource cleanup
    """

    def __init__(self, password: str, storage_path: str = None):
        self.password = password
        
        # Initialize secure storage
        storage_args = {"db_path": storage_path} if storage_path else {}
        self.store = SecureStorage(**storage_args)

        # Setup master key
        if not self.store.has_wrapped_master():
            self.store.create_and_store_master_key(password)
        else:
            self.store.unlock_with_password(password)

        # Ensure default profile exists
        if not self.load_user_profile():
            self.save_user_profile({"name": "New User", "timezone": "UTC"})

    # ---------------- Context Manager Support ----------------
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Guaranteed cleanup even if exceptions occur."""
        try:
            self.close()
        except Exception:
            pass  # Silent cleanup - critical for production

    # ---------------- Profile Management ----------------

    def save_user_profile(self, profile: Dict[str, Any]) -> None:
        """Save or update user profile with UTC timestamp."""
        if not isinstance(profile, dict):
            raise ValueError("Profile must be a dictionary")
            
        profile["last_updated"] = datetime.now(UTC).isoformat()
        self.store.put("user_profile", "profile", profile)

    def load_user_profile(self) -> Dict[str, Any]:
        """Load user profile with empty dict fallback."""
        return self.store.get("user_profile") or {}

    def update_user_setting(self, key: str, value: Any) -> bool:
        """Update specific user setting safely."""
        profile = self.load_user_profile()
        profile[key] = value
        self.save_user_profile(profile)
        return True

    # ---------------- Contacts Management ----------------

    def add_contact(self, contact_data: Dict[str, Any]) -> str:
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

        self.store.put(contact_id, "contact", contact_data)
        return contact_id

    def add_contacts_batch(self, contacts_list: List[Dict[str, Any]]) -> List[str]:
        """Add multiple contacts efficiently."""
        return [self.add_contact(contact) for contact in contacts_list]

    def update_contact(self, contact_id: str, contact_data: Dict[str, Any]) -> bool:
        """Update existing contact safely."""
        if not self.store.exists(contact_id):
            return False

        current = self.store.get(contact_id) or {}
        current.update(contact_data)
        current["last_updated"] = datetime.now(UTC).isoformat()

        self.store.put(contact_id, "contact", current)
        return True

    def list_contacts(self) -> List[Dict[str, Any]]:
        """List all contacts with full data."""
        return [item["obj"] for item in self.store.list_by_type("contact")]

    def get_contact(self, contact_id: str) -> Optional[Dict[str, Any]]:
        """Get specific contact by ID."""
        return self.store.get(contact_id)

    def search_contacts(self, query: str) -> List[Dict[str, Any]]:
        """Efficiently search contacts by multiple fields."""
        query = query.lower()
        search_fields = ["name", "email", "notes", "phone"]
        results = []
        
        for contact in self.list_contacts():
            if any(query in str(contact.get(field, '')).lower() 
                   for field in search_fields):
                results.append(contact)
        return results

    # ---------------- Events Management ----------------

    def add_event(self, event_data: Dict[str, Any]) -> str:
        """Add a new event with validation."""
        required = ["title", "datetime"]
        for field in required:
            if field not in event_data:
                raise ValueError(f"Event missing required field: {field}")

        # Validate datetime and prevent past events
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

        self.store.put(event_id, "event", event_data)
        return event_id

    def list_events(self) -> List[Dict[str, Any]]:
        """List all events."""
        return [item["obj"] for item in self.store.list_by_type("event")]

    def list_upcoming_events(self, days: int = 30) -> List[Dict[str, Any]]:
        """List upcoming events within specified days."""
        now = datetime.now(UTC)
        cutoff = now + timedelta(days=days)

        events = []
        for event in self.list_events():
            try:
                event_time = parse_iso_datetime(event["datetime"])
                if now <= event_time <= cutoff:
                    events.append(event)
            except (KeyError, ValueError):
                continue  # Skip invalid events

        return sorted(events, key=lambda e: parse_iso_datetime(e["datetime"]))

    def delete_event(self, event_id: str) -> bool:
        """Delete an event by ID."""
        return self.store.delete(event_id)

    # ---------------- System Management ----------------

    def get_system_stats(self) -> Dict[str, Any]:
        """Get comprehensive system statistics with error safety."""
        profile = self.load_user_profile()
        
        # Safe integrity check
        try:
            integrity = self.store.verify_integrity()
        except Exception:
            integrity = False

        return {
            "user": {
                "name": profile.get("name", "Not Set"),
                "email": profile.get("email", "Not Set"),
                "last_updated": profile.get("last_updated")
            },
            "counts": {
                "contacts": len(self.list_contacts()),
                "total_events": len(self.list_events()),
                "upcoming_7d": len(self.list_upcoming_events(7))
            },
            "storage": {
                "integrity": integrity,
                "last_backup": None  # Could be tracked in metadata
            },
            "timestamp": datetime.now(UTC).isoformat()
        }

    def show_system_overview(self) -> None:
        """Display formatted system overview."""
        stats = self.get_system_stats()
        
        print("=== AI Assistant System Overview ===")
        print(f"User: {stats['user']['name']} ({stats['user']['email']})")
        print(f"Contacts: {stats['counts']['contacts']}")
        print(f"Upcoming Events (7d): {stats['counts']['upcoming_7d']}")
        print(f"Storage Health: {'OK' if stats['storage']['integrity'] else 'ISSUE'}")
        print(f"Last Updated: {stats['timestamp']}")

    def create_backup(self, backup_path: str) -> bool:
        """Create system backup safely."""
        try:
            return self.store.create_backup(backup_path)
        except Exception:
            return False

    def close(self) -> None:
        """Secure shutdown with exception safety."""
        try:
            self.store.close()
        except Exception:
            pass  # Silent cleanup


# --- Production Demo Usage ---
if __name__ == "__main__":
    # Using context manager for guaranteed cleanup
    with AssistantCore(password="secure_password_123") as assistant:
        
        # User profile
        assistant.save_user_profile({
            "name": "John Doe",
            "email": "john@example.com", 
            "timezone": "America/New_York",
            "preferences": {"notifications": True, "language": "en"}
        })

        # Batch contact addition
        contacts = [
            {
                "name": "Alice Smith",
                "email": "alice@example.com",
                "phone": "+1-555-0101",
                "notes": "Project collaborator"
            },
            {
                "name": "Bob Johnson",
                "email": "bob@example.com", 
                "phone": "+1-555-0102",
                "relation": "Friend"
            }
        ]
        
        contact_ids = assistant.add_contacts_batch(contacts)
        print(f"Added {len(contact_ids)} contacts")

        # Event with future datetime
        event_time = datetime.now(UTC) + timedelta(days=3)
        event_id = assistant.add_event({
            "title": "Project Review Meeting",
            "datetime": event_time.isoformat(),
            "location": "Conference Room A",
            "description": "Quarterly project review with stakeholders",
            "priority": "high"
        })
        print(f"Added event: {event_id}")

        # Demonstrate features
        assistant.show_system_overview()
        
        print("\n=== Search Results ===")
        project_contacts = assistant.search_contacts("project")
        print(f"Contacts with 'project': {len(project_contacts)}")
        
        print("\n=== Upcoming Events ===")
        upcoming = assistant.list_upcoming_events(7)
        for event in upcoming:
            print(f"- {event['title']} at {event['datetime']}")

    print("Assistant shut down successfully")
