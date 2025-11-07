# proactive_comm.py
from __future__ import annotations

"""
ProactiveCommunicator - [UPGRADED] Enterprise-Grade Proactive Agent

This module is now a true, "indefinitely active" daemon.
- A dedicated scheduler loop (_scheduler_loop) acts as the system's "heartbeat,"
  loading tasks from the persistent queue.
- A pool of workers (_worker_loop) executes tasks from a high-speed
  in-memory queue.
- All Hologram calls are now safe and robust.
- Removed flawed 'integrate_with_interaction' to enforce a one-way data flow.
"""

import asyncio
import time
import uuid
import random
import logging
import traceback
import json
import inspect  # Added for safe hologram calls
from typing import Dict, Any, Optional, List, Tuple, Callable, Iterable

try:
    import aiohttp
except Exception:
    aiohttp = None  # optional adapter dependency

logger = logging.getLogger("AARIA.Proactive")
logger.addHandler(logging.NullHandler())

# Time utility fallbacks
try:
    from .time_utils import parse_to_timestamp, to_local_iso, now_ts  # type: ignore
    _HAS_TIMEUTILS = True
except Exception:
    _HAS_TIMEUTILS = False
    def now_ts() -> float: return time.time()
    def parse_to_timestamp(text: str, prefer_future: bool = True) -> Optional[float]:
        try:
            import re
            m = re.search(r"in\s+(\d+)\s*(s|sec|secs|seconds|m|min|mins|minutes|h|hr|hours)?", text, flags=re.I)
            if m:
                val = int(m.group(1)); unit = (m.group(2) or "s").lower()
                if unit.startswith("s"): return time.time() + val
                if unit.startswith("m"): return time.time() + val * 60
                if unit.startswith("h"): return time.time() + val * 3600
        except Exception: pass
        return None
    def to_local_iso(ts: float) -> str:
        import datetime
        try: return datetime.datetime.fromtimestamp(ts).isoformat()
        except Exception: return str(ts)


# ---------------------------
# Default Adapters
# ---------------------------
class LoggingAdapter:
    """Simple adapter used by default - logs sends and returns a simulated receipt."""
    def __init__(self, name: str = "logging"):
        self.name = name
        self._log = logging.getLogger(f"AARIA.Proactive.Adapter.{name}")
    def send(self, envelope: Dict[str, Any]):
        self._log.info("[%s] deliver -> channel=%s subject=%s payload=%s", self.name,
                       envelope.get("channel"), envelope.get("subject_identity"), envelope.get("payload"))
        async def _sim():
            await asyncio.sleep(0.02)
            return True, f"{self.name}_receipt_{int(now_ts()*1000)}"
        return _sim()


class HTTPWebhookAdapter:
    """Example HTTP webhook adapter: deliver envelope via POST."""
    def __init__(self, config: Dict[str, Any]):
        self.config = config or {}
        self._log = logging.getLogger("AARIA.Proactive.Adapter.webhook")
        self._session = None
    async def _ensure_session(self):
        if aiohttp is None: raise RuntimeError("aiohttp is required for HTTPWebhookAdapter")
        if not self._session:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.config.get("timeout", 10)))
    async def send(self, envelope: Dict[str, Any]) -> Tuple[bool, str]:
        await self._ensure_session()
        url = self.config.get("url")
        if not url: return False, "no_webhook_url"
        payload = {
            "id": envelope.get("id"), "channel": envelope.get("channel"),
            "subject_identity": envelope.get("subject_identity"),
            "payload": envelope.get("payload"),
            "meta": {"created_at": envelope.get("created_at")}
        }
        headers = self.config.get("headers", {"Content-Type": "application/json"})
        try:
            async with self._session.post(url, json=payload, headers=headers) as resp:
                text = await resp.text()
                if 200 <= resp.status < 300:
                    return True, f"webhook:{resp.status}"
                return False, f"webhook:{resp.status}:{text[:200]}"
        except Exception as e:
            self._log.exception("Webhook send failed")
            return False, str(e)
    async def close(self):
        if self._session:
            try: await self._session.close()
            except Exception: pass


# ---------------------------
# ProactiveCommunicator
# ---------------------------
class ProactiveCommunicator:
    PERSIST_KEY_QUEUE = "proactive_queue_v2"
    PERSIST_KEY_LOGS = "proactive_logs_v2"
    PERSIST_KEY_DEAD = "proactive_dead_v2"

    def __init__(
        self,
        core: Any,
        hologram_call_fn: Optional[Callable[..., Any]] = None,
        *,
        concurrency: int = 2,
        max_attempts: int = 5,
        base_backoff: float = 3.0,
        jitter: float = 1.0,
        scheduler_interval_seconds: int = 5, # <-- [NEW] How often the "heartbeat" runs
        rate_limit_per_minute: int = 120,
        persist_debounce: float = 0.25
    ):
        self.core = core
        self.hologram_call = hologram_call_fn # This is passed in from main.py
        self.store = getattr(core, "store", None) or getattr(core, "secure_store", None)
        if not self.store:
            logger.error("ProactiveCommunicator: CRITICAL: No core.store found. Persistence is disabled.")
        
        self.concurrency = max(1, int(concurrency))
        self.max_attempts = max(1, int(max_attempts))
        self.base_backoff = float(base_backoff)
        self.jitter = float(jitter)
        self.scheduler_interval_seconds = max(1, int(scheduler_interval_seconds))
        self.rate_limit_per_minute = max(1, int(rate_limit_per_minute))
        self.persist_debounce = float(persist_debounce)

        # [UPGRADED] Queues are now split for performance
        self._in_queue: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue() # Fast, in-memory-only
        self._persisted_queue: List[Dict[str, Any]] = [] # Durable, on-disk list
        self._dead_letter_queue: List[Dict[str, Any]] = []
        
        self._queue_lock = asyncio.Lock()
        self._last_persist = 0.0
        self._persist_task: Optional[asyncio.Task] = None
        self._worker_tasks: List[asyncio.Task] = []
        self._scheduler_task: Optional[asyncio.Task] = None # [NEW]
        self._running = False

        self.adapters: Dict[str, Any] = {}
        self._channel_counters: Dict[str, List[float]] = {}
        self._logs: List[Dict[str, Any]] = []
        self._logs_lock = asyncio.Lock()

        self.metrics = {"queued": 0, "sent": 0, "failed": 0, "attempts": 0, "avg_delivery_time": 0.0}

        self.register_adapter("logging", LoggingAdapter("proactive_logging"))
        logger.debug("ProactiveCommunicator constructed (start() must be awaited)")

    # ---------------------------
    # [NEW] Safe Hologram Wrappers
    # ---------------------------
    async def _safe_holo_spawn(self, node_id: str, node_type: str, label: str, size: int, source_id: str, link_id: str) -> bool:
        if not self.hologram_call: return False
        try:
            maybe = self.hologram_call("spawn_and_link", node_id=node_id, node_type=node_type, label=label, size=size, source_id=source_id, link_id=link_id)
            if inspect.iscoroutine(maybe): await maybe
            return True
        except Exception: return False
    async def _safe_holo_set_active(self, node_name: str):
        if not self.hologram_call: return False
        try:
            maybe = self.hologram_call("set_node_active", node_name)
            if inspect.iscoroutine(maybe): await maybe
            return True
        except Exception: return False
    async def _safe_holo_set_idle(self, node_name: str):
        if not self.hologram_call: return False
        try:
            maybe = self.hologram_call("set_node_idle", node_name)
            if inspect.iscoroutine(maybe): await maybe
            return True
        except Exception: return False
    async def _safe_holo_despawn(self, node_id: str, link_id: str):
        if not self.hologram_call: return False
        try:
            maybe = self.hologram_call("despawn_and_unlink", node_id, link_id)
            if inspect.iscoroutine(maybe): await maybe
            return True
        except Exception: return False

    # ---------------------------
    # Lifecycle
    # ---------------------------
    async def start(self):
        """Start scheduler and workers, and load persisted queue."""
        if self._running:
            logger.debug("ProactiveCommunicator already running")
            return
        self._running = True
        
        await self._load_persisted_queue()
        await self._load_dead_letter_queue()
        
        # [NEW] Start the scheduler loop
        self._scheduler_task = asyncio.create_task(self._scheduler_loop())
        
        # Start workers
        for _ in range(self.concurrency):
            t = asyncio.create_task(self._worker_loop())
            self._worker_tasks.append(t)
            
        logger.info("ProactiveCommunicator started with 1 scheduler and %d workers", len(self._worker_tasks))

    async def stop(self):
        """Stop workers, scheduler, persist queue & logs, close adapters."""
        if not self._running:
            logger.debug("ProactiveCommunicator.stop() called when not running")
            return
        self._running = False

        # Cancel scheduler
        if self._scheduler_task:
            self._scheduler_task.cancel()
        
        # Cancel worker tasks
        for t in list(self._worker_tasks):
            t.cancel()
            
        # Wait for all tasks to exit
        all_tasks = self._worker_tasks + ([self._scheduler_task] if self._scheduler_task else [])
        await asyncio.gather(*all_tasks, return_exceptions=True)
        self._worker_tasks.clear()
        self._scheduler_task = None

        await self._persist_queue(force=True)
        await self._persist_logs()
        await self._persist_dead_letter()

        for adapter in list(self.adapters.values()):
            close_m = getattr(adapter, "close", None)
            if callable(close_m):
                try:
                    maybe = close_m()
                    if asyncio.iscoroutine(maybe): await maybe
                except Exception:
                    logger.debug("Adapter close() failed", exc_info=True)

        logger.info("ProactiveCommunicator stopped")

    # ---------------------------
    # Public scheduling API (remains same)
    # ---------------------------
    async def schedule_reminder(
        self,
        text: str,
        when_text: Optional[str] = None,
        when_ts: Optional[float] = None,
        subject_identity: str = "owner_primary",
        channel: str = "logging",
        metadata: Optional[Dict[str, Any]] = None,
        priority: int = 50,
        batch_group: Optional[str] = None,
        visibility: str = "confidential",
    ) -> str:
        metadata = metadata or {}
        if when_ts:
            ts = float(when_ts)
        elif when_text:
            ts = None
            try:
                ts = parse_to_timestamp(when_text, prefer_future=True) if _HAS_TIMEUTILS else parse_to_timestamp(when_text)
            except Exception:
                ts = parse_to_timestamp(when_text)
            if ts is None:
                metadata["schedule_parse_failed"] = True
                ts = now_ts() + 5.0
        else:
            ts = now_ts() + 1.0

        if ts < now_ts() - 1.0:
            metadata["schedule_adjusted_from_past"] = True
            ts = now_ts() + 1.0

        payload = {"text": text, "meta": metadata}
        return await self.enqueue_message(
            subject_identity=subject_identity,
            channel=channel,
            payload=payload,
            priority=priority,
            batch_group=batch_group,
            deliver_after=ts,
            visibility=visibility,
        )

    async def enqueue_message(
        self,
        subject_identity: str,
        channel: str,
        payload: Dict[str, Any],
        priority: int = 50,
        batch_group: Optional[str] = None,
        deliver_after: Optional[float] = None,
        visibility: str = "confidential",
    ) -> str:
        """
        [UPGRADED]
        Core enqueue API. This now *only* writes to the durable _persisted_queue.
        The _scheduler_loop will move it to the _in_queue later.
        """
        envelope = {
            "id": f"pmsg_{int(now_ts() * 1000)}_{uuid.uuid4().hex[:6]}",
            "subject_identity": subject_identity,
            "channel": channel,
            "payload": payload,
            "priority": int(priority),
            "batch_group": batch_group,
            "attempts": 0,
            "max_attempts": self.max_attempts,
            "next_attempt_at": float(deliver_after or now_ts()),
            "created_at": now_ts(),
            "visibility": visibility,
            "last_error": None,
        }

        allowed = await self._authorize_enqueue(subject_identity, channel, visibility)
        if not allowed:
            logger.warning("Authorization failed for enqueue: %s %s", subject_identity, channel)
            raise PermissionError("Not authorized to enqueue proactive messages for this subject/visibility.")

        async with self._queue_lock:
            self._persisted_queue.append(envelope)
            # Sort persisted queue by next_attempt_at then priority
            self._persisted_queue.sort(key=lambda e: (e.get("next_attempt_at", 0), -e.get("priority", 0)))
            self.metrics["queued"] = len(self._persisted_queue)
            # [DELETED] No longer puts to _in_queue. The scheduler does that.
        
        await self._persist_queue_debounced()

        logger.debug("Enqueued proactive envelope %s channel=%s for %s", envelope["id"], channel, subject_identity)
        return envelope["id"]

    # ---------------------------
    # Worker & delivery
    # ---------------------------
    
    # --- [NEW] ---
    # The "Heartbeat" loop. Stays active indefinitely.
    async def _scheduler_loop(self):
        """
        [NEW] The "Heartbeat" loop.
        Periodically scans the durable _persisted_queue and moves any
        ready messages into the high-speed in-memory _in_queue for the workers.
        """
        logger.info("Proactive scheduler loop started.")
        while self._running:
            try:
                now = now_ts()
                async with self._queue_lock:
                    moved_count = 0
                    # Iterate in reverse to safely pop items
                    for i in range(len(self._persisted_queue) - 1, -1, -1):
                        # The queue is sorted, so we check from the beginning (earliest first)
                        # Wait, no, the sort key is (time, -priority).
                        # Let's re-sort just to be safe, then check.
                        self._persisted_queue.sort(key=lambda e: (e.get("next_attempt_at", 0), -e.get("priority", 0)))
                        
                        ready_to_move = []
                        remaining = []
                        
                        for env in self._persisted_queue:
                            if env.get("next_attempt_at", 0) <= now:
                                ready_to_move.append(env)
                            else:
                                remaining.append(env)
                        
                        self._persisted_queue = remaining
                        
                        if ready_to_move:
                            for env in ready_to_move:
                                await self._in_queue.put(env)
                            moved_count = len(ready_to_move)
                            self.metrics["queued"] = len(self._persisted_queue)
                            await self._persist_queue_debounced() # Save the now-shorter list
                            
                    if moved_count > 0:
                        logger.debug(f"Scheduler moved {moved_count} messages to in-memory queue.")

            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Proactive scheduler loop error (non-fatal)")
            
            await asyncio.sleep(self.scheduler_interval_seconds)
        logger.info("Proactive scheduler loop stopped.")

    # --- [UPGRADED] ---
    # The worker is now much simpler. It just waits for the in-memory queue.
    async def _worker_loop(self):
        """
        [UPGRADED] Worker loop.
        This now *only* consumes from the fast in-memory _in_queue.
        It no longer sleeps or checks times.
        """
        logger.debug("Proactive worker started")
        while self._running:
            try:
                envelope = await self._in_queue.get()
                
                # check rate limiting
                if not self._allow_send(envelope["channel"]):
                    envelope["next_attempt_at"] = now_ts() + 5.0 # Rate limit, try again in 5s
                    await self._reschedule_envelope(envelope)
                    continue

                await self._holo_spawn(envelope) # Now safe
                start = now_ts()
                ok, receipt = await self._deliver_envelope(envelope)
                elapsed = now_ts() - start
                await self._holo_idle(envelope) # Now safe

                envelope["attempts"] = envelope.get("attempts", 0) + 1
                self.metrics["attempts"] = self.metrics.get("attempts", 0) + 1

                if ok:
                    self.metrics["sent"] = self.metrics.get("sent", 0) + 1
                    # [FIXED] No need to remove from persisted queue, scheduler already did
                    # await self._remove_persisted_envelope(envelope["id"]) # <-- DELETED
                    await self._log_delivery(envelope, True, receipt, elapsed)
                    sent_count = max(1, self.metrics["sent"])
                    prev_avg = self.metrics.get("avg_delivery_time", 0.0)
                    self.metrics["avg_delivery_time"] = (prev_avg * (sent_count - 1) + elapsed) / sent_count
                else:
                    self.metrics["failed"] = self.metrics.get("failed", 0) + 1
                    envelope["last_error"] = receipt or "unknown_adapter_error"
                    if envelope["attempts"] >= envelope.get("max_attempts", self.max_attempts):
                        await self._move_to_dead_letter(envelope)
                        await self._log_delivery(envelope, False, receipt, elapsed, final=True)
                    else:
                        backoff = self.base_backoff * (2 ** (envelope["attempts"] - 1))
                        backoff += random.uniform(0, self.jitter)
                        envelope["next_attempt_at"] = now_ts() + backoff
                        await self._reschedule_envelope(envelope) # Put back in durable queue

                await asyncio.sleep(0) # Yield control
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Proactive worker loop encountered exception")
                await asyncio.sleep(1.0)
        logger.debug("Proactive worker exiting")

    # --- [UPGRADED] ---
    # Removed the flawed `deliver_hook` logic to enforce one-way data flow.
    # --- [END UPGRADE] ---
    async def _deliver_envelope(self, envelope: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        [UPGRADED]
        Deliver *only* using registered adapters.
        Removed flawed 'deliver_hook' and 'persona.notify' logic.
        """
        channel = envelope.get("channel")
        adapter = self.adapters.get(channel)
        
        if not adapter:
            return False, f"no_delivery_adapter_for_channel:{channel}"
            
        try:
            send_fn = getattr(adapter, "send", None) or adapter
            try:
                maybe = send_fn(envelope)
            except TypeError:
                maybe = adapter(envelope) # Fallback for simple callables

            if asyncio.iscoroutine(maybe):
                res = await maybe
            else:
                res = maybe

            # normalize result
            if isinstance(res, tuple) and len(res) >= 2:
                ok, receipt = bool(res[0]), str(res[1])
                return ok, receipt
            if isinstance(res, bool):
                return res, "adapter_bool"
            if isinstance(res, str):
                return True, res
            return bool(res), None

        except Exception as e:
            logger.exception("Delivery adapter raised exception")
            return False, str(e)

    # ---------------------------
    # Persistence (remains mostly the same, just uses self.store)
    # ---------------------------
    async def _persist_queue(self, force: bool = False):
        if not self.store:
            logger.debug("No store available to persist proactive queue.")
            return
        try:
            payload = {"queue": list(self._persisted_queue), "ts": now_ts()}
            put = getattr(self.store, "put", None)
            if put:
                try:
                    maybe = put(self.PERSIST_KEY_QUEUE, "proactive", payload)
                except TypeError:
                    maybe = put(self.PERSIST_KEY_QUEUE, payload)
                if asyncio.iscoroutine(maybe): await maybe
            elif hasattr(self.store, "__setitem__"):
                self.store[self.PERSIST_KEY_QUEUE] = payload
            self._last_persist = now_ts()
            logger.debug("Proactive queue persisted (count=%d).", len(self._persisted_queue))
        except Exception:
            logger.exception("Failed to persist proactive queue")

    async def _load_persisted_queue(self):
        if not self.store:
            logger.debug("No store available to load proactive queue.")
            return
        try:
            get = getattr(self.store, "get", None)
            maybe = None
            if get:
                try: maybe = get(self.PERSIST_KEY_QUEUE)
                except TypeError: maybe = get(self.PERSIST_KEY_QUEUE)
            else:
                maybe = self.store.get(self.PERSIST_KEY_QUEUE) if hasattr(self.store, "get") else None

            data = await maybe if asyncio.iscoroutine(maybe) else maybe
            if data and isinstance(data, dict) and "queue" in data:
                async with self._queue_lock:
                    self._persisted_queue = data.get("queue", []) or []
                    self.metrics["queued"] = len(self._persisted_queue)
                    # [DELETED] Do not load into _in_queue here. The scheduler will.
                logger.info("Loaded persisted proactive queue (count=%d)", len(self._persisted_queue))
        except Exception:
            logger.exception("Failed to load persisted proactive queue")

    async def _remove_persisted_envelope(self, envelope_id: str):
        # This is now redundant, as the scheduler loop removes it.
        # We leave it as a safeguard for cases where a worker
        # moves something to dead-letter.
        async with self._queue_lock:
            before = len(self._persisted_queue)
            self._persisted_queue = [e for e in self._persisted_queue if e.get("id") != envelope_id]
            after = len(self._persisted_queue)
            self.metrics["queued"] = after
            if before != after:
                await self._persist_queue_debounced()

    async def _reschedule_envelope(self, envelope: Dict[str, Any]):
        """Puts a failed envelope back into the durable _persisted_queue."""
        async with self._queue_lock:
            # We add it back to the main list for the scheduler to pick up later
            self._persisted_queue.append(envelope)
            self._persisted_queue.sort(key=lambda e: (e.get("next_attempt_at", 0), -e.get("priority", 0)))
            self.metrics["queued"] = len(self._persisted_queue)
        await self._persist_queue_debounced()

    async def _persist_queue_debounced(self):
        now = now_ts()
        if now - self._last_persist < self.persist_debounce:
            if self._persist_task and not self._persist_task.done():
                return
            self._persist_task = asyncio.create_task(self._delayed_persist(self.persist_debounce))
            return
        await self._persist_queue()

    async def _delayed_persist(self, delay: float):
        await asyncio.sleep(delay)
        await self._persist_queue()

    # ---------------------------
    # Dead-letter handling (remains the same)
    # ---------------------------
    async def _move_to_dead_letter(self, envelope: Dict[str, Any]):
        async with self._queue_lock:
            envelope_copy = dict(envelope)
            envelope_copy["moved_to_dead_at"] = now_ts()
            self._dead_letter_queue.append(envelope_copy)
            await self._persist_dead_letter()
        logger.warning("Envelope moved to dead-letter: %s", envelope.get("id"))

    async def _persist_dead_letter(self):
        if not self.store: return
        try:
            put = getattr(self.store, "put", None)
            payload = {"dead": list(self._dead_letter_queue), "ts": now_ts()}
            if put:
                try: maybe = put(self.PERSIST_KEY_DEAD, "proactive_dead", payload)
                except TypeError: maybe = put(self.PERSIST_KEY_DEAD, payload)
                if asyncio.iscoroutine(maybe): await maybe
            elif hasattr(self.store, "__setitem__"):
                self.store[self.PERSIST_KEY_DEAD] = payload
            logger.debug("Dead-letter persisted (count=%d)", len(self._dead_letter_queue))
        except Exception:
            logger.exception("Failed to persist dead-letter queue")

    async def _load_dead_letter_queue(self):
        if not self.store: return
        try:
            get = getattr(self.store, "get", None)
            maybe = None
            if get:
                try: maybe = get(self.PERSIST_KEY_DEAD)
                except TypeError: maybe = get(self.PERSIST_KEY_DEAD)
            else:
                maybe = self.store.get(self.PERSIST_KEY_DEAD) if hasattr(self.store, "get") else None
            data = await maybe if asyncio.iscoroutine(maybe) else maybe
            if data and isinstance(data, dict):
                self._dead_letter_queue = data.get("dead", []) or []
                logger.info("Loaded dead-letter queue (count=%d)", len(self._dead_letter_queue))
        except Exception:
            logger.exception("Failed to load dead-letter queue")

    # ---------------------------
    # Logging (remains the same)
    # ---------------------------
    async def _log_delivery(self, envelope: Dict[str, Any], success: bool, receipt: Optional[str], latency: float, final: bool = False):
        entry = {
            "id": f"log_{int(now_ts() * 1000)}_{uuid.uuid4().hex[:6]}",
            "envelope_id": envelope.get("id"), "subject_identity": envelope.get("subject_identity"),
            "channel": envelope.get("channel"), "success": bool(success), "receipt": receipt,
            "latency": latency, "attempts": envelope.get("attempts"), "final": bool(final),
            "ts": now_ts(), "payload_meta": envelope.get("payload", {}),
        }
        async with self._logs_lock:
            self._logs.append(entry)
            if len(self._logs) > 5000:
                self._logs = self._logs[-5000:]
        await self._persist_logs_debounced()

    async def _persist_logs(self):
        if not self.store: return
        try:
            put = getattr(self.store, "put", None)
            payload = {"logs": list(self._logs), "ts": now_ts()}
            if put:
                try: maybe = put(self.PERSIST_KEY_LOGS, "proactive_logs", payload)
                except TypeError: maybe = put(self.PERSIST_KEY_LOGS, payload)
                if asyncio.iscoroutine(maybe): await maybe
            elif hasattr(self.store, "__setitem__"):
                self.store[self.PERSIST_KEY_LOGS] = payload
            logger.debug("Proactive logs persisted (count=%d)", len(self._logs))
        except Exception:
            logger.exception("Failed to persist proactive logs")

    async def _persist_logs_debounced(self):
        await self._persist_logs()

    # ---------------------------
    # Rate limiting (remains the same)
    # ---------------------------
    def _allow_send(self, channel: str) -> bool:
        now = now_ts()
        window = 60.0
        lst = self._channel_counters.setdefault(channel, [])
        lst = [t for t in lst if now - t < window]
        self._channel_counters[channel] = lst
        if len(lst) >= self.rate_limit_per_minute:
            return False
        self._channel_counters[channel].append(now)
        return True

    # ---------------------------
    # Hologram helpers (UPGRADED)
    # ---------------------------
    async def _holo_spawn(self, envelope: Dict[str, Any]):
        if not self.hologram_call: return
        try:
            nid = f"pmsg_{envelope.get('id')[:8]}"
            lid = f"link_{nid}"
            spawned = await self._safe_holo_spawn(nid, "comm", f"Proactive:{envelope.get('channel')}", 3, "ProactiveCommunicator", lid)
            if spawned:
                await self._safe_holo_set_active("ProactiveCommunicator")
                envelope["_holo_node_id"] = nid # Store for cleanup
                envelope["_holo_link_id"] = lid
        except Exception:
            logger.debug("Hologram spawn failed (non-fatal)")

    async def _holo_idle(self, envelope: Dict[str, Any]):
        if not self.hologram_call: return
        try:
            await self._safe_holo_set_idle("ProactiveCommunicator")
            nid = envelope.get("_holo_node_id")
            lid = envelope.get("_holo_link_id")
            if nid and lid:
                await self._safe_holo_despawn(nid, lid)
        except Exception:
            pass

    # ---------------------------
    # Authorization (remains the same)
    # ---------------------------
    async def _authorize_enqueue(self, subject_identity: str, channel: str, visibility: str) -> bool:
        """Authorize enqueueing of proactive messages."""
        if subject_identity in ("owner_primary", "owner"):
            try:
                prefs = {}
                if hasattr(self.core, "load_user_profile"):
                    maybe = self.core.load_user_profile()
                    prefs = await maybe if asyncio.iscoroutine(maybe) else (maybe or {})
                elif hasattr(self.core, "persona") and getattr(self.core, "persona", None) is not None:
                    persona_core = getattr(self.core, "persona", None)
                    load_m = getattr(persona_core, "load_user_profile", None)
                    if load_m:
                        maybe = load_m()
                        prefs = await maybe if asyncio.iscoroutine(maybe) else (maybe or {})
                if not prefs:
                    core_store = getattr(self.core, "store", None) or getattr(self.core, "secure_store", None)
                    if core_store and hasattr(core_store, "get"):
                        for k in ("user_profile", "user_profile_small", "profile", "owner_profile"):
                            try:
                                data = await core_store.get(k) if asyncio.iscoroutinefunction(core_store.get) else core_store.get(k)
                                if data: prefs = data or {}; break
                            except Exception: continue
                
                p = (prefs.get("proactive_prefs") if isinstance(prefs, dict) else None) or prefs
                if isinstance(p, dict):
                    if p.get("enabled") is True:
                        if channel not in p.get("channels", ["sms", "push", "tts", "call", "logging"]):
                            return False
                        qh = p.get("quiet_hours")
                        if qh and isinstance(qh, (list, tuple)) and len(qh) == 2:
                            try:
                                import datetime
                                nowh = datetime.datetime.now().hour
                                start, end = int(qh[0]), int(qh[1])
                                if start <= end:
                                    if start <= nowh < end: return False
                                else:
                                    if nowh >= start or nowh < end: return False
                            except Exception: pass
                        return True
                
                try:
                    cfg_flag = False
                    if isinstance(getattr(self.core, "config", None), dict):
                        cfg_flag = bool(self.core.config.get("allow_proactive_by_default", False))
                    if not cfg_flag:
                        cfg_flag = bool(getattr(self.core, "allow_proactive_by_default", False))
                    if cfg_flag:
                        logger.debug("Proactive allowed by core.allow_proactive_by_default fallback (owner path).")
                        return True
                except Exception: pass
                return False
            except Exception: return False

        sec = getattr(self.core, "security_orchestrator", None)
        if sec and hasattr(sec, "authorize_proactive"):
            try:
                resp = sec.authorize_proactive(subject_identity, channel, visibility)
                return await resp if asyncio.iscoroutine(resp) else bool(resp)
            except Exception: return False
        return False

    async def enable_proactive_defaults(self, subject_identity: str = "owner_primary", channels: Optional[List[str]] = None, quiet_hours: Optional[Tuple[int,int]] = None):
        """Persist simple proactive_prefs for owner so proactive can run."""
        channels = channels or ["logging", "console"]
        small = {"proactive_prefs": {"enabled": True, "channels": channels}}
        try:
            if hasattr(self.core, "save_user_profile"):
                maybe = self.core.save_user_profile(small)
                if asyncio.iscoroutine(maybe): await maybe
                logger.info("Wrote proactive prefs via core.save_user_profile")
                return True
            if hasattr(self.core, "persona") and hasattr(self.core.persona, "core") and hasattr(self.core.persona.core, "save_user_profile"):
                maybe = self.core.persona.core.save_user_profile(small)
                if asyncio.iscoroutine(maybe): await maybe
                logger.info("WWrote proactive prefs via persona.core.save_user_profile")
                return True
        except Exception:
            logger.debug("save_user_profile not available or failed; falling back to store")

        if self.store and hasattr(self.store, "put"):
            try:
                key = "user_profile_small"
                maybe = self.store.put(key, "profile", small) # Use correct (k,t,v) signature
                if asyncio.iscoroutine(maybe): await maybe
                logger.info("Persisted proactive prefs to store key=%s", key)
                return True
            except Exception as e:
                logger.warning("Failed to persist proactive defaults to store: %s", e)
                return False
        return False

    # ---------------------------
    # Utilities & admin APIs
    # ---------------------------
    async def list_queue(self, include_payload: bool = False) -> List[Dict[str, Any]]:
        async with self._queue_lock:
            out = []
            for e in self._persisted_queue:
                copy = {k: v for k, v in e.items() if include_payload or k != "payload"}
                out.append(copy)
            return out
    async def list_logs(self, limit: int = 100) -> List[Dict[str, Any]]:
        async with self._logs_lock:
            return list(self._logs[-limit:])
    async def list_dead_letter(self, limit: int = 100) -> List[Dict[str, Any]]:
        async with self._queue_lock:
            return list(self._dead_letter_queue[-limit:])
    async def _rescue_enqueue_by_id(self, envelope_id: str) -> bool:
        async with self._queue_lock:
            for env in self._persisted_queue:
                if env.get("id") == envelope_id:
                    await self._in_queue.put(env)
                    return True
        return False
    async def retry_dead_letter(self, envelope_id: str) -> bool:
        """Attempt to resurrect a dead-letter envelope back into active queue (resets attempts)."""
        async with self._queue_lock:
            for i, env in enumerate(self._dead_letter_queue):
                if env.get("id") == envelope_id:
                    env["attempts"] = 0
                    env["next_attempt_at"] = now_ts() + 1.0
                    self._persisted_queue.append(env)
                    self._dead_letter_queue.pop(i)
                    self._persisted_queue.sort(key=lambda e: (e.get("next_attempt_at", 0), -e.get("priority", 0)))
                    await self._persist_queue()
                    await self._persist_dead_letter()
                    # It will be picked up by the scheduler, no need to put in _in_queue
                    return True
        return False
    async def clear_queue(self):
        async with self._queue_lock:
            self._persisted_queue.clear()
            self.metrics["queued"] = 0
            await self._persist_queue()
    async def clear_dead_letter(self):
        async with self._queue_lock:
            self._dead_letter_queue.clear()
            await self._persist_dead_letter()
    def register_adapter(self, channel: str, adapter: Any):
        self.adapters[channel] = adapter
        logger.info("Adapter registered for channel '%s'", channel)
    def unregister_adapter(self, channel: str):
        if channel in self.adapters:
            self.adapters.pop(channel, None)
            logger.info("Adapter unregistered for channel '%s'", channel)

    # --- [DELETED] ---
    # This method creates a circular dependency and is a flawed pattern.
    # The correct flow is: Cognition -> Autonomy -> Proactive -> Adapter
    # NOT Proactive -> InteractionCore
    # def integrate_with_interaction(self, interaction_core: Any): ...
    # --- [END DELETED] ---

    # ---------------------------
    # Health & metrics
    # ---------------------------
    async def health_check(self) -> Dict[str, Any]:
        """Return a dictionary with status, queue sizes, dead-letter count, recent errors, and adapter health hints."""
        try:
            adapters_info = {}
            for name, adapter in self.adapters.items():
                adapters_info[name] = {
                    "type": adapter.__class__.__name__,
                    "has_close": callable(getattr(adapter, "close", None))
                }
            recent_errors = []
            async with self._logs_lock:
                for log in reversed(self._logs[-50:]):
                    if not log.get("success"):
                        recent_errors.append(log)
            return {
                "status": "running" if self._running else "stopped",
                "queued": len(self._persisted_queue),
                "in_memory_pending": self._in_queue.qsize() if self._in_queue else 0,
                "dead_letter": len(self._dead_letter_queue),
                "metrics": dict(self.metrics),
                "adapters": adapters_info,
                "recent_errors": recent_errors[:10],
                "ts": now_ts()
            }
        except Exception as e:
            logger.exception("Health check failed")
            return {"status": "error", "error": str(e), "ts": now_ts()}

    def get_metrics(self) -> Dict[str, Any]:
        """Return a snapshot of metrics (non-async)."""
        return dict(self.metrics)

# End of file