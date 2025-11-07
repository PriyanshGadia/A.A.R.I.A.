import asyncio
import logging
import time
import uuid
from typing import Any, Dict

logger = logging.getLogger("AARIA.ProactiveStub")
logger.setLevel(logging.INFO)

DEFAULT_INTERVAL = int(__import__("os").environ.get("PROACTIVE_INTERVAL_SECS", "300"))

class ProactiveCommunicator:
    """
    Conservative Proactive Communicator (stub).
    - Runs a periodic check to create 'ideas' (logged), does NOT autopost.
    - Provides enqueue_message(subject, channel, payload) to simulate sending.
    - Designed to be safe by default: all outbound sends must be explicitly approved.
    """
    def __init__(self, persona, hologram_call_fn=None, concurrency: int = 1, interval: int = DEFAULT_INTERVAL):
        self.persona = persona
        self.hologram_call_fn = hologram_call_fn
        self.interval = interval
        self._running = False
        self._task = None
        self._concurrency = concurrency

    async def start(self):
        if self._running:
            return
        self._running = True
        logger.info("ProactiveCommunicator (stub) starting...")
        self._task = asyncio.create_task(self._loop())

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("ProactiveCommunicator (stub) stopped")

    async def enqueue_message(self, subject_id: str, channel: str, payload: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """
        Simulate enqueueing a message for send.
        Returns a queued object (id + meta). Does not actually send.
        """
        queued_id = f"pq_{int(time.time()*1000)}_{uuid.uuid4().hex[:6]}"
        logger.info(f"[ProactiveStub] enqueue_message queued={queued_id} subject={subject_id} channel={channel} payload_summary={str(payload)[:120]}")
        return {"status": "queued", "id": queued_id, "subject": subject_id, "channel": channel, "timestamp": time.time()}

    async def _loop(self):
        while self._running:
            try:
                # Examine memory state (best-effort)
                mem_state = {}
                try:
                    mem_state = self.persona.debug_memory_state()
                except Exception:
                    pass

                idea = {
                    "timestamp": time.time(),
                    "summary": f"Proactive idea: memory_count={mem_state.get('total_memories', '?')}, important={mem_state.get('important_memories','?')}",
                    "actionable": False
                }
                # Log the idea and optionally persist into persona memories as low-importance
                logger.info(f"[ProactiveStub] Idea: {idea['summary']}")
                try:
                    # store as low-importance memory for traceability (best-effort)
                    asyncio.create_task(self.persona.store_memory("proactive:summary", idea["summary"], importance=1, metadata={"proactive": True}))
                except Exception:
                    pass

                # Sleep until next interval
                await asyncio.sleep(self.interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Proactive loop error: {e}")
                await asyncio.sleep(10)