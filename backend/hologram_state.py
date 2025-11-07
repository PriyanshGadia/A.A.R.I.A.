# hologram_state.py
"""
Hologram state manager: nodes & links lifecycle helper functions,
single source of color truth, lightweight subscribers for WS broadcast.
"""

import asyncio
import time
import uuid
import logging
from typing import Dict, Any, List, Callable, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

COMPONENT_COLORS = {
    "persona": "#00aaff",
    "cognition": "#d400ff",
    "autonomy": "#f0e100",
    "memory": "#bfc7cf",
    "security": "#00ff9d",
    "input": "#ffffff",
    "task": "#00ff00",
    "default": "#aaaaaa"
}
ERROR_COLOR = "#ff0000"

_nodes: Dict[str, Dict[str, Any]] = {}
_links: Dict[str, Dict[str, Any]] = {}
_subscribers: List[Callable[[List[Dict[str, Any]], List[Dict[str, Any]]], None]] = []
_lock = asyncio.Lock()

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
    nodes = list(_nodes.values())
    links = list(_links.values())
    for cb in list(_subscribers):
        try:
            maybe = cb(nodes, links)
            if asyncio.iscoroutine(maybe):
                asyncio.create_task(maybe)
        except Exception:
            pass

async def subscribe(callback: Callable[[List[Dict[str, Any]], List[Dict[str, Any]]], None]):
    async with _lock:
        _subscribers.append(callback)
    await _notify_subscribers()

async def unsubscribe(callback: Callable[[List[Dict[str, Any]], List[Dict[str, Any]]], None]):
    async with _lock:
        if callback in _subscribers:
            _subscribers.remove(callback)

async def spawn_node(node_type: str, component: str, label: str, size: int = 8,
                     metadata: Optional[dict] = None, node_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Create a node. If node_id is passed and not already used, that id will be assigned.
    Otherwise, a unique id is generated.
    """
    async with _lock:
        nid = node_id or _uniq(node_type.lower())
        if nid in _nodes:
            # existing node -> update metadata/label/size and return
            node = _nodes[nid]
            node.update({
                "label": label,
                "type": node_type,
                "component": component,
                "size": size,
                "metadata": metadata or node.get("metadata", {}),
                "spawn_time": _now()
            })
            # ensure color consistent
            node["color"] = _component_color(component)
            node["state"] = "spawning"
            node["intensity"] = 0.45
            _nodes[nid] = node
        else:
            # create fresh node
            color = _component_color(component)
            node = {
                "id": nid,
                "label": label,
                "type": node_type,
                "component": component,
                "color": color,
                "size": size,
                "intensity": 0.45,
                "state": "spawning",
                "spawn_time": _now(),
                "metadata": metadata or {}
            }
            _nodes[nid] = node
    await _notify_subscribers()
    asyncio.create_task(_finish_spawn(nid))
    return _nodes[nid]

def _ensure_id(prefix: str, explicit_id: Optional[str] = None) -> str:
    if explicit_id:
        return explicit_id
    return f"{prefix}_{uuid.uuid4().hex[:8]}"

async def initialize_base_state():
    """
    Ensure the system's core nodes exist with deterministic IDs so other systems
    (identity manager, interaction, etc.) can reliably reference them.
    Returns a dict mapping id names to node objects.
    """
    core_defs = [
        ("CognitionCore", "Cognition", "cognition", 12),
        ("PersonaCore", "Persona", "persona", 12),
        ("AutonomyCore", "Autonomy", "autonomy", 12),
        ("SecurityCore", "Security", "security", 12),
        ("Memory", "Memory", "memory", 10),
    ]

    created = {}
    for explicit_id, label, component, size in core_defs:
        # If already exists, do nothing; spawn_node will update if id exists
        node = await spawn_node(node_type=label, component=component, label=label, size=size, node_id=explicit_id)
        created[explicit_id] = node

    # Optionally create links between these cores (example)
    try:
        # simple fully-connected-ish small graph for visibility
        core_ids = list(created.keys())
        for i in range(len(core_ids)):
            for j in range(i + 1, len(core_ids)):
                src = core_ids[i]
                tgt = core_ids[j]
                # ensure a link doesn't already exist between these ids
                if not any((l["source"] == src and l["target"] == tgt) or (l["source"] == tgt and l["target"] == src) for l in _links.values()):
                    await link_nodes(src, tgt, dot_density=2, intensity=0.06)
    except Exception:
        # non-fatal; we log but don't raise
        pass

    await _notify_subscribers()
    return created

async def _finish_spawn(node_id: str):
    await asyncio.sleep(0.9)
    async with _lock:
        node = _nodes.get(node_id)
        if not node:
            return
        if node["state"] == "spawning":
            node["state"] = "idle"
            node["intensity"] = max(0.15, node.get("intensity", 0.2))
    await _notify_subscribers()

async def link_nodes(source_id: str, target_id: str, dot_density: int = 3, intensity: float = 0.08, link_id: Optional[str] = None) -> Dict[str, Any]:
    async with _lock:
        if source_id not in _nodes or target_id not in _nodes:
            raise KeyError("source or target node not found")
        src_color = _nodes[source_id]["color"]
        tgt_color = _nodes[target_id]["color"]
        lid = link_id or _uniq("link")
        # If link id already exists, update it instead of duplicating
        if lid in _links:
            link = _links[lid]
            link.update({
                "source": source_id,
                "target": target_id,
                "gradient": [src_color, tgt_color],
                "intensity": intensity,
                "dot_density": max(1, int(dot_density)),
                "state": link.get("state", "idle")
            })
            _links[lid] = link
        else:
            link = {
                "id": lid,
                "source": source_id,
                "target": target_id,
                "gradient": [src_color, tgt_color],
                "intensity": intensity,
                "dot_density": max(1, int(dot_density)),
                "state": "idle"
            }
            _links[lid] = link
    await _notify_subscribers()
    return link


async def set_node_active(node_id: str, activity_multiplier: float = 1.25):
    async with _lock:
        node = _nodes.get(node_id)
        if not node:
            return
        node["state"] = "active"
        node["intensity"] = min(1.0, node.get("intensity", 0.3) * activity_multiplier)
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
        node["intensity"] = max(0.12, node.get("intensity", 0.2))
        for lid, l in _links.items():
            if l["source"] == node_id or l["target"] == node_id:
                l["state"] = "idle"
                l["dot_density"] = max(1, int(l.get("dot_density", 3) * 0.7))
    await _notify_subscribers()

async def set_node_error(node_id: str, reason: Optional[str] = None):
    async with _lock:
        node = _nodes.get(node_id)
        if not node:
            return
        node["state"] = "error"
        node["metadata"]["error_reason"] = reason
        node["color"] = ERROR_COLOR
        node["intensity"] = 1.0
        for lid, l in _links.items():
            if l["source"] == node_id or l["target"] == node_id:
                l["state"] = "error"
    await _notify_subscribers()

async def despawn_node(node_id: str):
    async with _lock:
        node = _nodes.get(node_id)
        if not node:
            return
        node["state"] = "deactivated"
    await _notify_subscribers()
    await asyncio.sleep(1.2)
    async with _lock:
        to_remove = [lid for lid, l in _links.items() if l["source"] == node_id or l["target"] == node_id]
        for lid in to_remove:
            _links.pop(lid, None)
        _nodes.pop(node_id, None)
    await _notify_subscribers()

async def despawn_link(link_id: str):
    async with _lock:
        _links.pop(link_id, None)
    await _notify_subscribers()

# --- add this helper above spawn_and_link ---

# --- REPLACE existing _resolve_node_identifier with this improved version ---

def _resolve_node_identifier(identifier: str) -> Optional[str]:
    """
    Robust resolver for node identifiers.
    Tries multiple case / underscore / hyphen / whitespace variants and returns the
    first matching node id from _nodes, otherwise None.
    """
    if not identifier:
        return None

    # normalize whitespace and punctuation a little
    ident = identifier.strip()
    if not ident:
        return None

    # direct hit (fast path)
    if ident in _nodes:
        return ident

    candidates = set()

    # Common transforms
    # 1) snake_case / hyphen-case -> CamelCase, JoinedLower, Lower, Title
    base = ident.replace('-', '_')
    parts = [p for p in base.split('_') if p]
    if parts:
        # CamelCase: SecurityCore
        candidates.add(''.join(p.title() for p in parts))
        # Joined lowercase: securitycore
        candidates.add(''.join(parts).lower())
        # Joined title-ish: Securitycore
        candidates.add(''.join(p.capitalize() for p in parts))
        # snake kept normalized
        candidates.add('_'.join(parts))
    # 2) simple case variants
    candidates.add(ident.title())
    candidates.add(ident.capitalize())
    candidates.add(ident.lower())
    candidates.add(ident.replace('_', ''))
    candidates.add(ident.replace('-', ''))

    # 3) try removing common suffix/prefix artifacts
    candidates.add(ident.rstrip('_').rstrip('-'))
    candidates.add(ident.lstrip('_').lstrip('-'))

    # Check candidates in a stable order derived from a predictable sort,
    # but prefer exact camelcase if present.
    ordered = sorted(candidates, key=lambda s: (s.lower(), -len(s)))

    for cand in ordered:
        if cand in _nodes:
            return cand

    # final fallback: try any node whose lowercase equals cand lowercase (rare)
    lower_map = {k.lower(): k for k in _nodes.keys()}
    if ident.lower() in lower_map:
        return lower_map[ident.lower()]

    return None


# --- REPLACE existing spawn_and_link with this robust version ---

async def spawn_and_link(*args, **kwargs):
    """
    Robust spawn-and-link wrapper. If the provided source identifier cannot
    be resolved immediately, attempt to initialize base state once and retry.
    Returns (node, link) on success.
    """
    # Accept common keyword args
    node_id = kwargs.pop("node_id", None)
    link_id = kwargs.pop("link_id", None)
    metadata = kwargs.pop("metadata", None)
    link_intensity = kwargs.pop("link_intensity", kwargs.pop("intensity", 0.06))
    dot_density = kwargs.pop("dot_density", kwargs.pop("dots", 2))
    gradient = kwargs.pop("gradient", None)

    node_type = None
    component = None
    label = None
    size = None
    source_id = None

    # Normalise positional signatures
    if len(args) >= 5:
        node_type, component, label, size, source_id = args[:5]
    elif len(args) == 4:
        node_type, label, size, source_id = args
        component = None
    else:
        node_type = kwargs.get("node_type") or kwargs.get("type")
        component = kwargs.get("component")
        label = kwargs.get("label")
        size = kwargs.get("size")
        source_id = kwargs.get("source_id") or kwargs.get("source")

    if not node_type or not label or size is None or not source_id:
        raise TypeError(
            "spawn_and_link requires (node_type, [component], label, size, source_id). "
            "Accepted forms: spawn_and_link(node_type, component, label, size, source_id) "
            "or spawn_and_link(node_type, label, size, source_id)."
        )

    if not component:
        component = "input"

    resolved_source = None
    async with _lock:
        if source_id in _nodes:
            resolved_source = source_id
        else:
            resolved_source = _resolve_node_identifier(source_id)

    # If unresolved, attempt one idempotent initialize_base_state() then retry resolution.
    if not resolved_source:
        logger.info(f"spawn_and_link: source_id '{source_id}' not found; attempting initialize_base_state() and retry.")
        try:
            # initialize_base_state is async and idempotent in your file
            await initialize_base_state()
        except Exception as e:
            logger.debug(f"spawn_and_link: initialize_base_state() raised: {e}", exc_info=True)

        async with _lock:
            if source_id in _nodes:
                resolved_source = source_id
            else:
                resolved_source = _resolve_node_identifier(source_id)

    if not resolved_source:
        # Final failure — don't swallow, but raise with a clear message
        logger.error(f"spawn_and_link: source_id '{source_id}' not found after retry.")
        raise KeyError(f"spawn_and_link: source_id '{source_id}' not found")

    source_id = resolved_source

    # Create or update node
    node = await spawn_node(
        node_type=node_type,
        component=component,
        label=label,
        size=size,
        metadata=metadata,
        node_id=node_id
    )

    # Create the link
    link = await link_nodes(
        source_id,
        node["id"],
        dot_density=dot_density,
        intensity=link_intensity,
        link_id=link_id
    )

    if gradient:
        async with _lock:
            link["gradient"] = gradient
            _links[link["id"]] = link
        await _notify_subscribers()

    return node, link


def snapshot() -> Dict[str, Any]:
    return {"nodes": list(_nodes.values()), "links": list(_links.values())}

# Verification-specific functions for TOTP flow
async def spawn_verification_node(verification_data: dict):
    """Create hologram state for TOTP verification"""
    logger.info(f"Creating hologram state for verification: {verification_data}")

    # Ensure core nodes present; if not present this will raise or create them earlier via init
    if "SecurityCore" not in _nodes:
        # attempt a non-blocking creation of core nodes (idempotent)
        try:
            await initialize_base_state()
        except Exception:
            logger.debug("spawn_verification_node: initialize_base_state failed or already executed")

    # Create verification node with deterministic node id (optional)
    node = await spawn_node(
        node_type="Verification",
        component="security",
        label="TOTP Verification",
        size=6,
        metadata={"verification_type": "TOTP", **(verification_data or {})},
        node_id=None  # keep it unique unless you want a deterministic id
    )

    # Link from SecurityCore -> verification node (use link_nodes which accepts link_id)
    try:
        link = await link_nodes("SecurityCore", node["id"], dot_density=5, intensity=0.15)
    except KeyError as e:
        # if SecurityCore still missing, attempt to ensure and retry once
        logger.warning("spawn_verification_node: SecurityCore missing, attempting to initialize base state and retry.")
        try:
            await initialize_base_state()
            link = await link_nodes("SecurityCore", node["id"], dot_density=5, intensity=0.15)
        except Exception as e2:
            logger.error(f"Failed to create verification link after retry: {e2}", exc_info=True)
            # clean up node and re-raise
            await despawn_node(node["id"])
            raise

    return {
        "node_id": node["id"],
        "link_id": link["id"]
    }


async def despawn_verification_node(node_id: str, link_id: str):
    """Clean up hologram state after TOTP verification"""
    logger.info(f"Cleaning up verification hologram state - node: {node_id}, link: {link_id}")
    
    # Despawn the link first
    await despawn_link(link_id)
    
    # Then despawn the node
    await despawn_node(node_id)
    
    return True

# >>> ADD THIS (compatibility helper)
 # --- BEGIN: Additional convenience API for other modules / backward compatibility ---

async def get_all_nodes_links() -> Dict[str, Any]:
    """
    Async-friendly accessor used by server broadcaster.
    Returns a fresh snapshot of nodes and links.
    """
    async with _lock:
        return {"nodes": list(_nodes.values()), "links": list(_links.values())}

async def update_link_intensity(link_id: str, intensity: float):
    """
    Smoothly update a link's intensity (used by PersonaCore hologram tasks).
    If link missing, log a warning rather than raising (non-fatal).
    """
    async with _lock:
        link = _links.get(link_id)
        if not link:
            logger.warning(f"update_link_intensity: link '{link_id}' not found.")
            return False
        # clamp intensity to sensible range
        try:
            new_int = float(intensity)
        except Exception:
            logger.warning(f"update_link_intensity: invalid intensity value: {intensity}")
            return False
        link["intensity"] = max(0.0, min(2.0, new_int))
        _links[link_id] = link
    await _notify_subscribers()
    return True

async def update_link_gradient(link_id: str, gradient: List[str]):
    """
    Update the color gradient of a link.
    """
    async with _lock:
        link = _links.get(link_id)
        if not link:
            logger.warning(f"update_link_gradient: link '{link_id}' not found.")
            return False
        if not isinstance(gradient, list) or len(gradient) < 2:
            logger.warning("update_link_gradient: gradient must be a list of at least two color strings")
            return False
        link["gradient"] = gradient
        _links[link_id] = link
    await _notify_subscribers()
    return True

async def update_node_metadata(node_id: str, metadata: dict):
    """
    Merge metadata into an existing node; create the node if it doesn't exist (optional).
    Returns the node or None if failed.
    """
    async with _lock:
        node = _nodes.get(node_id)
        if not node:
            logger.warning(f"update_node_metadata: node '{node_id}' not found.")
            return None
        if not isinstance(metadata, dict):
            logger.warning("update_node_metadata: metadata must be a dict")
            return None
        node["metadata"].update(metadata)
        _nodes[node_id] = node
    await _notify_subscribers()
    return node

async def despawn_and_unlink(node_id: str, link_id: str):
    """
    Backwards-compatible helper: remove a link and node (if present) in the safe order.
    Returns True on success.
    """
    # attempt link removal first, then node (both are idempotent)
    try:
        await despawn_link(link_id)
    except Exception as e:
        logger.debug(f"despawn_and_unlink: despawn_link failed: {e}")
    try:
        await despawn_node(node_id)
    except Exception as e:
        logger.debug(f"despawn_and_unlink: despawn_node failed: {e}")
    return True

# --- END additions ---

