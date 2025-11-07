"""
device_commands.py - CLI Commands for Device Management
"""

import logging
from typing import Dict, Any
from device_types import DeviceType
import hologram_state  # <-- ADD THIS IMPORT
import uuid            # <-- ADD THIS IMPORT
import asyncio         # <-- ADD THIS IMPORT

logger = logging.getLogger("AARIA.Device")

class DeviceCommands:
    """CLI commands for device management"""
    
    def __init__(self, security_orchestrator):
        self.security = security_orchestrator
        self.device_manager = security_orchestrator.access_control.device_manager
    
    async def process_device_command(self, command: str, params: Dict[str, Any], 
                                   identity) -> str:
        """Process device management commands"""
        
        # --- NEW: Hologram Task Tracking ---
        node_id = f"dev_task_{uuid.uuid4().hex[:6]}"
        link_id = f"link_auto_{node_id}"
        
        try:
            # --- NEW: Spawn "PC Task" node ---
            await hologram_state.spawn_and_link(
                node_id=node_id, 
                node_type="task", 
                label=f"PC Command: {command}", 
                size=5,
                source_id="AutonomyCore", # Linked from Autonomy
                link_id=link_id
            )
            # Set the new node to "active" (transitions from white to green)
            await hologram_state.set_node_active(node_id)
            # --- End New Block ---

            # (Original method logic starts here)
            if command == "register_device":
                return await self._handle_register_device(params, identity)
            elif command == "list_devices":
                return await self._handle_list_devices(params, identity)
            elif command == "add_trusted_network":
                return await self._handle_add_trusted_network(params, identity)
            elif command == "remove_device":
                return await self._handle_remove_device(params, identity)
            else:
                await hologram_state.set_node_error(node_id) # Set error state
                return f"❌ Unknown device command: {command}"
            # (End of original method logic)

        except Exception as e:
            # --- NEW: Set node to ERROR state on failure ---
            await hologram_state.set_node_error(node_id)
            logger.error(f"Device command {command} failed: {e}")
            return f"❌ Error processing device command: {str(e)}"
            # --- End New Block ---

        finally:
            # --- NEW: Despawn node after a short delay ---
            # We add a small delay so the node doesn't vanish instantly
            await asyncio.sleep(2) 
            await hologram_state.despawn_and_unlink(node_id, link_id)
            # --- End New Block ---
    
    async def _handle_register_device(self, params: Dict[str, Any], identity) -> str:
        """Register a new device: register_device device_id=my_phone type=mobile"""
        device_id = params.get("device_id", "").strip()
        device_type = params.get("type", "").strip()
        
        if not device_id or not device_type:
            return "❌ device_id and type are required"
        
        valid_types = ["home_pc", "mobile", "tablet", "public_terminal"]
        if device_type not in valid_types:
            return f"❌ Invalid device type. Available: {', '.join(valid_types)}"
        
        try:
            await self.device_manager.register_device(device_id, device_type, {
                "registered_by": identity.preferred_name,
                "registration_time": self._get_current_timestamp()
            })
            return f"✅ Registered device: {device_id} as {device_type}"
        except Exception as e:
            return f"❌ Failed to register device: {str(e)}"
    
    async def _handle_list_devices(self, params: Dict[str, Any], identity) -> str:
        """List all registered devices"""
        devices = self.device_manager.device_fingerprints
        
        if not devices:
            return "📱 No devices registered"
        
        device_list = []
        for device_id, info in devices.items():
            device_info = f"• {device_id} - {info.get('type', 'unknown')}"
            if info.get('last_seen'):
                device_info += f" (last seen: {self._format_timestamp(info['last_seen'])})"
            device_list.append(device_info)
        
        return "📱 Registered Devices:\n" + "\n".join(device_list)
    
    async def _handle_add_trusted_network(self, params: Dict[str, Any], identity) -> str:
        """Add trusted network: add_trusted_network network=192.168.1.0/24"""
        network = params.get("network", "").strip()
        
        if not network:
            return "❌ Network CIDR is required (e.g., 192.168.1.0/24)"
        
        try:
            await self.device_manager.add_trusted_network(network)
            return f"✅ Added trusted network: {network}"
        except Exception as e:
            return f"❌ Failed to add trusted network: {str(e)}"
    
    async def _handle_remove_device(self, params: Dict[str, Any], identity) -> str:
        """Remove a device: remove_device device_id=my_phone"""
        device_id = params.get("device_id", "").strip()
        
        if not device_id:
            return "❌ device_id is required"
        
        if device_id in self.device_manager.device_fingerprints:
            del self.device_manager.device_fingerprints[device_id]
            await self.device_manager._save_device_fingerprints()
            return f"✅ Removed device: {device_id}"
        else:
            return f"❌ Device not found: {device_id}"
    
    def _get_current_timestamp(self):
        import time
        return time.time()
    
    def _format_timestamp(self, timestamp):
        from datetime import datetime
        return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M")