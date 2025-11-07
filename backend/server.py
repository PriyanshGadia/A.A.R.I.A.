import asyncio
import socketio
import uvicorn
import logging
import sys
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from datetime import datetime

# Import A.A.R.I.A.'s main system components
from main import AARIASystem
from interaction_core import InboundMessage

# --- NEW: Import the Async Hologram State Manager ---
import hologram_state

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("AARIA.Server")

# --- Global A.A.R.I.A. Instance & Verification State ---
aaria_system: AARIASystem | None = None
verified_sids: set[str] = set()

# --- Background Task for Hologram State ---
state_broadcaster_task: asyncio.Task | None = None

# --- REVISED: This function now sends the new Node/Link data ---
async def broadcast_system_state():
    """
    A background task that continuously gathers and broadcasts
    A.A.R.I.A.'s core metrics to all verified UI clients.
    This will power the "living" hologram.
    """
    global sio, verified_sids
    while True:
        try:
            # Run ~2 times per second
            await asyncio.sleep(0.5)

            if not verified_sids:
                continue  # No one to send to

            # 1. Get a snapshot of the hologram state (synchronous snapshot())
            snapshot = hologram_state.snapshot()
            nodes = snapshot.get("nodes", [])
            links = snapshot.get("links", [])

            # 2. Assemble the state payload
            system_state = {
                "nodes": nodes,
                "links": links
            }

            # 3. Broadcast to all verified clients
            for sid in list(verified_sids):
                try:
                    await sio.emit('system_state_update', system_state, to=sid)
                except Exception as e:
                    logger.debug(f"Failed to emit system_state_update to {sid}: {e}")

        except asyncio.CancelledError:
            logger.info("System state broadcaster is stopping.")
            break
        except Exception as e:
            logger.error(f"Error in system state broadcaster: {e}", exc_info=True)
            await asyncio.sleep(10)

async def ensure_core_nodes_present():
    """
    Ensures the deterministic core nodes are present in hologram_state.
    Idempotent — safe to call repeatedly.
    Returns a set of current node ids after ensuring.
    """
    try:
        snapshot = hologram_state.snapshot()
        existing_ids = {n["id"] for n in snapshot.get("nodes", [])}

        required = {
            "CognitionCore": ("Cognition", "cognition", 12),
            "PersonaCore": ("Persona", "persona", 12),
            "AutonomyCore": ("Autonomy", "autonomy", 12),
            "SecurityCore": ("Security", "security", 12),
            "Memory": ("Memory", "memory", 10)
        }

        created = []
        for nid, (label, comp, size) in required.items():
            if nid not in existing_ids:
                logger.info(f"ensure_core_nodes_present: creating missing core node {nid}")
                # spawn_node supports node_id param per earlier patch
                try:
                    node = await hologram_state.spawn_node(label, comp, label, size, node_id=nid)
                    created.append(nid)
                except TypeError:
                    # older spawn_node signature — fallback to spawn_node without explicit id
                    node = await hologram_state.spawn_node(label, comp, label, size)
                    created.append(node["id"])
        if created:
            logger.info(f"ensure_core_nodes_present: created {created}")
        # return the post-check snapshot ids
        snapshot2 = hologram_state.snapshot()
        return {n["id"] for n in snapshot2.get("nodes", [])}
    except Exception as e:
        logger.error(f"ensure_core_nodes_present failed: {e}", exc_info=True)
        return set()


# --- A.A.R.I.A. Lifespan (Startup & Shutdown) Context Manager ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global aaria_system, state_broadcaster_task
    try:
        logger.info("🚀 A.A.R.I.A. Core System is starting up...")
        aaria_system = AARIASystem()
        initialized = await aaria_system.initialize()

        if not initialized:
            logger.critical("❌ A.A.R.I.A. Core failed to initialize. Check logs.")
            aaria_system = None
        else:
            logger.info("✅ A.A.R.I.A. Core System is online and ready.")

            # --- NEW: Try to initialize the hologram's base nodes/links ---
            try:
                if hasattr(hologram_state, "initialize_base_state"):
                    # If the hologram_state module provides an initializer, call it
                    await hologram_state.initialize_base_state()
                    logger.info("Hologram base state initialized via initialize_base_state().")
                else:
                    # Otherwise log a warning and proceed — UI will receive an empty snapshot until nodes spawn
                    logger.warning("hologram_state.initialize_base_state() not found — skipping automatic base state init.")
            except Exception as e:
                logger.error(f"Error initializing hologram base state: {e}", exc_info=True)

            # --- START THE HOLOGRAM BROADCASTER ---
            state_broadcaster_task = asyncio.create_task(broadcast_system_state())

    except Exception as e:
        logger.critical(f"❌ FATAL: A.A.R.I.A. startup failed: {e}", exc_info=True)
        aaria_system = None

    yield  # Server is now running

    # --- Server is shutting down ---
    if state_broadcaster_task:
        state_broadcaster_task.cancel()  # Stop the broadcaster

    if aaria_system:
        logger.info("🛑 A.A.R.I.A. Core System is shutting down...")
        await aaria_system.shutdown()
        logger.info("✅ A.A.R.I.A. Core System shutdown complete.")


# --- FastAPI & Socket.IO Setup ---
sio = socketio.AsyncServer(async_mode='asgi', cors_allowed_origins='*')
fastapi_app = FastAPI(lifespan=lifespan)
fastapi_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app = socketio.ASGIApp(sio, other_asgi_app=fastapi_app)

@fastapi_app.get("/debug/hologram")
async def debug_hologram():
    """
    Returns the current hologram snapshot. Use this to confirm whether
    the deterministic core nodes (CognitionCore, PersonaCore, etc.) exist.
    """
    try:
        snap = hologram_state.snapshot()
        return snap
    except Exception as e:
        logger.error(f"debug_hologram error: {e}", exc_info=True)
        return {"error": "failed to fetch snapshot", "details": str(e)}

# --- WebSocket Event Handlers ---
@sio.event
async def connect(sid: str, environ: dict):
    logger.info(f"🔌 New UI client connected: {sid} (Awaiting Verification) from IP: {environ.get('REMOTE_ADDR', 'N/A')}")


@sio.event
async def disconnect(sid: str):
    logger.info(f"🔌 UI client disconnected: {sid}")
    verified_sids.discard(sid)


@sio.event
async def verify_totp(sid: str, code: str):
    global aaria_system, verified_sids

    if not aaria_system:
        await sio.emit('system_error', {'error': 'A.A.R.I.A. Core is offline. Cannot verify.'}, to=sid)
        return

    # Make sure the hologram core nodes exist before verification attempts
    await ensure_core_nodes_present()

    try:
        identity_manager = aaria_system.components['security'].identity_manager
        # Try verification; if it raises the 'source or target node not found' error we will try once more after ensuring cores
        try:
            is_verified = await identity_manager.challenge_owner_verification(code)
        except Exception as inner_exc:
            txt = str(inner_exc)
            logger.debug(f"Initial TOTP verification error: {txt}")
            if "source or target node not found" in txt.lower():
                logger.info("verify_totp: missing hologram nodes detected during verification — ensuring cores and retrying once.")
                await ensure_core_nodes_present()
                try:
                    is_verified = await identity_manager.challenge_owner_verification(code)
                except Exception as inner_exc2:
                    logger.error(f"TOTP verification retry failed: {inner_exc2}", exc_info=True)
                    await sio.emit('system_error', {'error': 'Verification failed after retry.'}, to=sid)
                    return
            else:
                # non-related error — bubble out
                raise

        if is_verified:
            logger.info(f"🔐 Client {sid} verified successfully.")
            verified_sids.add(sid)

            await sio.emit('ui_init', {
                'message': 'Verification Successful. Welcome, Owner.',
                'layout': ['comms', 'security', 'insights', 'hologram']
            }, to=sid)

            await asyncio.sleep(1)

            greeting = "A.A.R.I.A. systems online. All core functions operational. Awaiting your command, Owner."
            await sio.emit('assistant_response', {
                'content': greeting,
                'metadata': {'sender': 'AARIA', 'timestamp': datetime.now().isoformat()}
            }, to=sid)
        else:
            logger.warning(f"Client {sid} provided invalid TOTP code: '{code}'")
            await sio.emit('system_error', {'error': 'Invalid TOTP Code'}, to=sid)

    except Exception as e:
        logger.error(f"TOTP verification error for client {sid}: {e}", exc_info=True)
        await sio.emit('system_error', {'error': 'Verification system error. Try again.'}, to=sid)


# --- NEW: Demo function for diagnostics ---
async def run_diagnostics_demo():
    """A helper function to simulate a long-running task and test ALL hologram states."""
    try:
        # Create a temporary source node to link test nodes to
        logger.info("DEMO: Creating temporary demo source node (AutonomyCore demo).")
        source = await hologram_state.spawn_node("AutonomyCore", "autonomy", "Autonomy Core (demo)", 10)

        # --- Test 1: Successful Task ---
        logger.info("DEMO: Spawning SUCCESSFUL node...")
        node_ok, link_ok = await hologram_state.spawn_and_link("Task", "task", "Test: Success", 6, source["id"])
        await hologram_state.set_node_active(source["id"])
        await asyncio.sleep(0.9)  # spawn transition

        await hologram_state.set_node_active(node_ok["id"])
        logger.info("DEMO: Node set to ACTIVE (green)")
        await asyncio.sleep(3)

        logger.info("DEMO: Despawning SUCCESSFUL node...")
        await hologram_state.set_node_idle(source["id"])
        await hologram_state.despawn_node(node_ok["id"])
        # remove link if hologram_state exposes despawn_link
        if hasattr(hologram_state, "despawn_link"):
            # find the generated link id (if returned earlier) — our spawn_and_link returned link_ok
            try:
                await hologram_state.despawn_link(link_ok["id"])
            except Exception:
                pass

        await asyncio.sleep(2)

        # --- Test 2: Failed Task ---
        logger.info("DEMO: Spawning FAILED node...")
        node_err, link_err = await hologram_state.spawn_and_link("Task", "task", "Test: Failure", 6, source["id"])
        await hologram_state.set_node_active(source["id"])
        await asyncio.sleep(0.9)

        await hologram_state.set_node_error(node_err["id"])
        logger.info("DEMO: Node set to ERROR (red)")
        await asyncio.sleep(4)

        logger.info("DEMO: Despawning FAILED node...")
        await hologram_state.set_node_idle(source["id"])
        await hologram_state.despawn_node(node_err["id"])
        if hasattr(hologram_state, "despawn_link"):
            try:
                await hologram_state.despawn_link(link_err["id"])
            except Exception:
                pass

    except Exception as e:
        logger.error(f"Error running diagnostics demo: {e}", exc_info=True)
    finally:
        # Clean up source node
        try:
            await hologram_state.despawn_node(source["id"])
        except Exception:
            pass


# --- REVISED: Handler for system commands ---
async def handle_system_command(sid: str, command: str):
    global aaria_system
    if not aaria_system or sid not in verified_sids:
        return  # Silently fail

    logger.info(f"Received system command: {command}")

    if command == "DIAGNOSTICS_REQUEST":
        # --- NEW: Run the demo task ---
        asyncio.create_task(run_diagnostics_demo())
        await sio.emit('assistant_response', {
            'content': 'Acknowledged. Running diagnostics...',
            'metadata': {'sender': 'AARIA', 'timestamp': datetime.now().isoformat()}
        }, to=sid)

    elif command == "SUSPEND_CORE":
        autonomy = aaria_system.components.get('autonomy')
        if autonomy:
            autonomy.autonomy_enabled = not autonomy.autonomy_enabled  # Toggle
            status = "SUSPENDED" if not autonomy.autonomy_enabled else "ACTIVE"
            await sio.emit('assistant_response', {
                'content': f"Autonomy Core is now {status}.",
                'metadata': {'sender': 'AARIA', 'timestamp': datetime.now().isoformat()}
            }, to=sid)


@sio.event
async def user_message(sid: str, data: dict):
    if sid not in verified_sids:
        await sio.emit('system_error', {'error': 'Authentication required.'}, to=sid)
        return

    if not aaria_system:
        await sio.emit('system_error', {'error': 'A.A.R.I.A. Core is offline.'}, to=sid)
        return

    try:
        user_input = data.get('content')
        if not user_input:
            return

        # Intercept dummy button commands
        if user_input in ["DIAGNOSTICS_REQUEST", "SUSPEND_CORE"]:
            await handle_system_command(sid, user_input)
            return

        logger.info(f"💬 Received message from UI ({sid}): {user_input}")

        inbound = InboundMessage(
            channel="aaria_shell",
            content=user_input,
            user_id=data.get('user_id', 'owner_primary'),
            session_id=sid,
            metadata={
                "device_id": "cli_terminal",
                "source": "private_terminal",
                "ip_address": "127.0.0.1",
                "user_name": data.get("user_name", "Owner")
            }
        )

        response_message = await aaria_system.components['interaction'].handle_inbound(inbound)

        if response_message:
            logger.info(f"🤖 Sending response to UI ({sid}): {response_message.content[:80]}{'...' if len(response_message.content) > 80 else ''}")
            await sio.emit('assistant_response', {
                'content': response_message.content,
                'metadata': response_message.metadata
            }, to=sid)

    except Exception as e:
        logger.error(f"Error processing message from {sid}: {e}", exc_info=True)
        await sio.emit('system_error', {'error': f'An internal error occurred: {e}'}, to=sid)


# --- Main Entry Point for Running the Server ---
if __name__ == "__main__":
    logger.info("Starting A.A.R.I.A. Backend Server with Uvicorn...")
    uvicorn.run(
        "server:app",
        host="127.0.0.1",
        port=1420,
        reload=True
    )
