# hologram_state.py
"""
Hologram state manager
- Manages nodes and links using the JSON schema described in the brief
- Provides spawn/link/activate/deactivate/error/despawn helpers with timings
- Provides subscription callbacks for broadcasting state (e.g. websocket server)
"""

import asyncio
import time
import uuid
from typing import Dict, Any, List, Callable, Optional

# ---------------------------------------------------------------------------
# Component color map: cool/dark palette, no red except for errors.
# Keep this single source of truth.
COMPONENT_COLORS = {
    "persona": "#00aaff",     # bright cyan / blue
    "cognition": "#d400ff",   # magenta / purple
    "autonomy": "#f0e100",    # muted yellow
    "memory": "#bfc7cf",      # cool light gray (useable against dark bg)
    "security": "#00ff9d",    # teal / seafoam
    "input": "#ffffff",       # white (contrasty)
    "task": "#00ff00",        # green
    "default": "#aaaaaa"
}
ERROR_COLOR = "#ff0000"

# Node states
NODE_STATES = ("spawning", "active", "idle", "error", "deactivated")

# Internal store
_nodes: Dict[str, Dict[str, Any]] = {}
_links: Dict[str, Dict[str, Any]] = {}

# Subscribers: functions that accept (nodes, links)
_subscribers: List[Callable[[List[Dict[str, Any]], List[Dict[str, Any]]], None]] = []

# Lock for concurrent access
_lock = asyncio.Lock()


# --------------------------- Utilities -------------------------------------
def _now() -> float:
    return time.time()


def _uniq(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _component_color(component: str) -> str:
    return COMPONENT_COLORS.get(component, COMPONENT_COLORS["default"])


def _node_base(node_type: str, component: str, label: str, size: int, metadata: Optional[dict] = None) -> Dict[str, Any]:
    color = _component_color(component)
    return {
        "id": _uniq(node_type.lower()),
        "label": label,
        "type": node_type,
        "component": component,
        "color": color,
        "size": size,
        "intensity": 0.0,
        "state": "spawning",
        "spawn_time": _now(),
        "metadata": metadata or {}
    }


async def _notify_subscribers():
    # push current snapshot to subscribers (non-blocking)
    nodes = list(_nodes.values())
    links = list(_links.values())
    for cb in list(_subscribers):
        # run each callback in background; subscribers should handle exceptions
        try:
            maybe_coro = cb(nodes, links)
            if asyncio.iscoroutine(maybe_coro):
                asyncio.create_task(maybe_coro)
        except Exception:
            # subscriber exceptions should not break state flow
            pass


# --------------------------- API -------------------------------------------
async def subscribe(callback: Callable[[List[Dict[str, Any]], List[Dict[str, Any]]], None]):
    """Register a subscriber callback; callback receives (nodes, links)."""
    async with _lock:
        _subscribers.append(callback)
    # immediately send the current snapshot
    await _notify_subscribers()


async def unsubscribe(callback: Callable[[List[Dict[str, Any]], List[Dict[str, Any]]], None]):
    async with _lock:
        if callback in _subscribers:
            _subscribers.remove(callback)


async def spawn_node(node_type: str, component: str, label: str, size: int = 8, metadata: Optional[dict] = None) -> Dict[str, Any]:
    """Spawn a new node in 'spawning' state. Frontend should animate white -> component color."""
    async with _lock:
        node = _node_base(node_type, component, label, size, metadata)
        # default base intensity mapping (small formula, tweakable)
        node["intensity"] = 0.25 if size < 8 else 0.45
        _nodes[node["id"]] = node
    await _notify_subscribers()
    # schedule auto transition to 'idle' after spawn animation finished (900ms)
    asyncio.create_task(_finish_spawn(node["id"]))
    return node


async def _finish_spawn(node_id: str):
    await asyncio.sleep(0.9)  # spawn transition duration specified in spec
    async with _lock:
        node = _nodes.get(node_id)
        if not node:
            return
        if node["state"] == "spawning":
            node["state"] = "idle"
            # ensure intensity is at base
            node["intensity"] = max(0.15, node.get("intensity", 0.2))
    await _notify_subscribers()


async def link_nodes(source_id: str, target_id: str, dot_density: int = 3, intensity: float = 0.08) -> Dict[str, Any]:
    """Create a link between two existing nodes and return link."""
    async with _lock:
        if source_id not in _nodes or target_id not in _nodes:
            raise KeyError("source or target node not found")
        src_color = _nodes[source_id]["color"]
        tgt_color = _nodes[target_id]["color"]
        link_id = _uniq("link")
        link = {
            "id": link_id,
            "source": source_id,
            "target": target_id,
            "gradient": [src_color, tgt_color],
            "intensity": intensity,
            "dot_density": max(1, int(dot_density)),
            "state": "idle"
        }
        _links[link_id] = link
    await _notify_subscribers()
    return link


async def set_node_active(node_id: str, activity_multiplier: float = 1.25):
    async with _lock:
        node = _nodes.get(node_id)
        if not node:
            return
        node["state"] = "active"
        # bump intensity with multiplier but cap at 1.0
        node["intensity"] = min(1.0, node.get("intensity", 0.3) * activity_multiplier)
        # set outgoing links active
        for lid, l in _links.items():
            if l["source"] == node_id or l["target"] == node_id:
                l["state"] = "active"
                l["dot_density"] = max(2, int(l.get("dot_density", 3) * 1.5))
    await _notify_subscribers()


async def set_node_idle(node_id: str):
    async with _lock:
        node = _nodes.get(node_id)
        if not node:
            return
        node["state"] = "idle"
        # intensity back to base
        node["intensity"] = max(0.12, node.get("intensity", 0.2))
        for lid, l in _links.items():
            if l["source"] == node_id or l["target"] == node_id:
                l["state"] = "idle"
                # gently reduce dot density
                l["dot_density"] = max(1, int(l.get("dot_density", 3) * 0.7))
    await _notify_subscribers()


async def set_node_error(node_id: str, reason: Optional[str] = None):
    async with _lock:
        node = _nodes.get(node_id)
        if not node:
            return
        node["state"] = "error"
        node["metadata"]["error_reason"] = reason
        # set error color; frontend should animate component color->red
        node["color"] = ERROR_COLOR
        node["intensity"] = 1.0
        # set links to error
        for lid, l in _links.items():
            if l["source"] == node_id or l["target"] == node_id:
                l["state"] = "error"
    await _notify_subscribers()


async def despawn_node(node_id: str):
    """Begin the deactivation animation; remove from store after grace period (1.2s)."""
    async with _lock:
        node = _nodes.get(node_id)
        if not node:
            return
        node["state"] = "deactivated"
        # fade to black is frontend's job; backend will remove after delay.
    await _notify_subscribers()
    # schedule removal after 1.2s
    await asyncio.sleep(1.2)
    async with _lock:
        # remove links referencing node
        to_remove = [lid for lid, l in _links.items() if l["source"] == node_id or l["target"] == node_id]
        for lid in to_remove:
            _links.pop(lid, None)
        _nodes.pop(node_id, None)
    await _notify_subscribers()


async def despawn_link(link_id: str):
    async with _lock:
        _links.pop(link_id, None)
    await _notify_subscribers()


# -------------------- convenience flows (composed ops) ----------------------
async def spawn_and_link(node_type: str, component: str, label: str, size: int, source_id: str):
    """
    Spawn a node and immediately link it to source_id (useful for MemoryRead/LLMQuery flows).
    Returns (node, link).
    """
    node = await spawn_node(node_type, component, label, size)
    link = await link_nodes(source_id, node["id"])
    return node, link


# ------------------------------ read helpers --------------------------------
def snapshot() -> Dict[str, Any]:
    return {"nodes": list(_nodes.values()), "links": list(_links.values())}
