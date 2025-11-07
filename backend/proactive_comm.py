# proactive_comm.py
from __future__ import annotations

"""
ProactiveCommunicator - production-grade proactive notification system for A.A.R.I.A

Features:
- Durable queue persisted via core.store or core.secure_store (async-aware)
- In-memory active queue + persisted queue for restart resilience
- Pluggable adapters (default LoggingAdapter + HTTPWebhookAdapter example)
- Exponential backoff with jitter and dead-letter handling
- Concurrency via asyncio worker pool
- Rate limiting per-channel (sliding window)
- Logs persisted and rotatable
- Health check + runtime metrics
- Integration helpers for PersonaCore / InteractionCore
- Safe async/sync adapter call handling
"""

import asyncio
import time
import uuid
import random
import logging
import traceback
import json
from typing import Dict, Any, Optional, List, Tuple, Callable, Iterable

try:
    import aiohttp
except Exception:
    aiohttp = None  # optional adapter dependency

logger = logging.getLogger("AARIA.Proactive")
logger.addHandler(logging.NullHandler())

# Time utility fallbacks (use your repo's time_utils if available)
try:
    from .time_utils import parse_to_timestamp, to_local_iso, now_ts  # type: ignore
    _HAS_TIMEUTILS = True
except Exception:
    _HAS_TIMEUTILS = False

    def now_ts() -> float:
        return time.time()

    def parse_to_timestamp(text: str, prefer_future: bool = True) -> Optional[float]:
        try:
            import re
            m = re.search(r"in\s+(\d+)\s*(s|sec|secs|seconds|m|min|mins|minutes|h|hr|hours)?", text, flags=re.I)
            if m:
                val = int(m.group(1))
                unit = (m.group(2) or "s").lower()
                if unit.startswith("s"):
                    return time.time() + val
                if unit.startswith("m"):
                    return time.time() + val * 60
                if unit.startswith("h"):
                    return time.time() + val * 3600
        except Exception:
            pass
        return None

    def to_local_iso(ts: float) -> str:
        import datetime
        try:
            return datetime.datetime.fromtimestamp(ts).isoformat()
        except Exception:
            return str(ts)


# ---------------------------
# Default Adapters
# ---------------------------
class LoggingAdapter:
    """Simple adapter used by default - logs sends and returns a simulated receipt."""

    def __init__(self, name: str = "logging"):
        self.name = name
        self._log = logging.getLogger(f"AARIA.Proactive.Adapter.{name}")

    def send(self, envelope: Dict[str, Any]):
        """Accept envelope; may be sync or return a coroutine for async compatibility."""
        self._log.info("[%s] deliver -> channel=%s subject=%s payload=%s", self.name,
                       envelope.get("channel"), envelope.get("subject_identity"), envelope.get("payload"))
        async def _sim():
            await asyncio.sleep(0.02)
            return True, f"{self.name}_receipt_{int(now_ts()*1000)}"
        return _sim()


class HTTPWebhookAdapter:
    """
    Example HTTP webhook adapter: deliver envelope via POST to a configured webhook URL.
    Config: {"url": "...", "headers": {...}, "timeout": 10}
    """
    def __init__(self, config: Dict[str, Any]):
        self.config = config or {}
        self._log = logging.getLogger("AARIA.Proactive.Adapter.webhook")
        self._session = None

    async def _ensure_session(self):
        if aiohttp is None:
            raise RuntimeError("aiohttp is required for HTTPWebhookAdapter")
        if not self._session:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.config.get("timeout", 10)))

    async def send(self, envelope: Dict[str, Any]) -> Tuple[bool, str]:
        await self._ensure_session()
        url = self.config.get("url")
        if not url:
            return False, "no_webhook_url"
        payload = {
            "id": envelope.get("id"),
            "channel": envelope.get("channel"),
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
            try:
                await self._session.close()
            except Exception:
                pass


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
        batch_window_seconds: int = 5,
        rate_limit_per_minute: int = 120,
        persist_debounce: float = 0.25,
        deliver_hook: Optional[Callable[[Dict[str, Any]], Any]] = None,
    ):
        """
        core: the assistant core (AssistantCore / PersonaCore). Persistence will use core.store or core.secure_store
        hologram_call_fn: optional function to create hologram nodes for activity visualization
        deliver_hook: fallback callable to deliver messages (persona.notify or interaction layer)
        """

        self.core = core
        self.hologram_call = hologram_call_fn

        # deliver_hook heuristics (callable or persona-like object)
        self.deliver_hook = deliver_hook or getattr(core, "deliver_proactive", None) or getattr(core, "persona", None) or getattr(core, "notify", None)

        self.concurrency = max(1, int(concurrency))
        self.max_attempts = max(1, int(max_attempts))
        self.base_backoff = float(base_backoff)
        self.jitter = float(jitter)
        self.batch_window_seconds = max(1, int(batch_window_seconds))
        self.rate_limit_per_minute = max(1, int(rate_limit_per_minute))
        self.persist_debounce = float(persist_debounce)

        # runtime queues
        self._in_queue: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue()
        self._persisted_queue: List[Dict[str, Any]] = []
        self._dead_letter_queue: List[Dict[str, Any]] = []
        self._queue_lock = asyncio.Lock()
        self._last_persist = 0.0
        self._persist_task: Optional[asyncio.Task] = None

        self._worker_tasks: List[asyncio.Task] = []
        self._running = False

        self.adapters: Dict[str, Any] = {}
        self._channel_counters: Dict[str, List[float]] = {}
        self._logs: List[Dict[str, Any]] = []
        self._logs_lock = asyncio.Lock()

        self.metrics = {"queued": 0, "sent": 0, "failed": 0, "attempts": 0, "avg_delivery_time": 0.0}

        # seed default adapters
        self.register_adapter("logging", LoggingAdapter("proactive_logging"))
        # typical channels you may want: sms, push, tts, call, webhook
        logger.debug("ProactiveCommunicator constructed (start() must be awaited)")

    # ---------------------------
    # Lifecycle
    # ---------------------------
    async def start(self):
        """Start workers and load persisted queue (idempotent)."""
        if self._running:
            logger.debug("ProactiveCommunicator already running")
            return
        self._running = True
        await self._load_persisted_queue()
        await self._load_dead_letter_queue()
        for _ in range(self.concurrency):
            t = asyncio.create_task(self._worker_loop())
            self._worker_tasks.append(t)
        logger.info("ProactiveCommunicator started with %d workers", len(self._worker_tasks))

    async def stop(self):
        """Stop workers, persist queue & logs, close adapters that support close()."""
        if not self._running:
            logger.debug("ProactiveCommunicator.stop() called when not running")
            return
        self._running = False

        # cancel worker tasks
        for t in list(self._worker_tasks):
            t.cancel()
        await asyncio.gather(*self._worker_tasks, return_exceptions=True)
        self._worker_tasks.clear()

        # persist everything
        await self._persist_queue(force=True)
        await self._persist_logs()
        await self._persist_dead_letter()

        # close adapters with close()
        for adapter in list(self.adapters.values()):
            close_m = getattr(adapter, "close", None)
            if callable(close_m):
                try:
                    maybe = close_m()
                    if asyncio.iscoroutine(maybe):
                        await maybe
                except Exception:
                    logger.debug("Adapter close() failed", exc_info=True)

        logger.info("ProactiveCommunicator stopped")

    # ---------------------------
    # Public scheduling API
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
        """
        High-level convenience for scheduling a human-friendly reminder.
        Accepts when_text like "in 5 minutes" (parsed by parse_to_timestamp).
        """
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
        envelope_id = await self.enqueue_message(
            subject_identity=subject_identity,
            channel=channel,
            payload=payload,
            priority=priority,
            batch_group=batch_group,
            deliver_after=ts,
            visibility=visibility,
        )
        return envelope_id

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
        Core enqueue API. Returns envelope id.
        Envelope structure is persisted; delivery workers pick it up.
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
            # dedupe by id is unlikely (id uses uuid) but keep deterministic ordering by priority/time
            self._persisted_queue.append(envelope)
            # sort persisted queue by next_attempt_at then priority
            self._persisted_queue.sort(key=lambda e: (e.get("next_attempt_at", 0), -e.get("priority", 0)))
            self.metrics["queued"] = len(self._persisted_queue)
            if envelope["next_attempt_at"] <= now_ts() + 1.0:
                await self._in_queue.put(envelope)
            await self._persist_queue_debounced()

        logger.debug("Enqueued proactive envelope %s channel=%s for %s", envelope["id"], channel, subject_identity)
        return envelope["id"]

    # ---------------------------
    # Worker & delivery
    # ---------------------------
    async def _worker_loop(self):
        logger.debug("Proactive worker started")
        while self._running:
            try:
                envelope = await self._in_queue.get()
                now = now_ts()
                if envelope.get("next_attempt_at", 0) > now:
                    await asyncio.sleep(max(0.0, envelope["next_attempt_at"] - now))

                # check rate limiting
                if not self._allow_send(envelope["channel"]):
                    envelope["next_attempt_at"] = now_ts() + 5.0
                    await self._reschedule_envelope(envelope)
                    continue

                # hologram (optional)
                await self._holo_spawn(envelope)

                start = now_ts()
                ok, receipt = await self._deliver_envelope(envelope)
                elapsed = now_ts() - start

                await self._holo_idle(envelope)

                envelope["attempts"] = envelope.get("attempts", 0) + 1
                self.metrics["attempts"] = self.metrics.get("attempts", 0) + 1

                if ok:
                    self.metrics["sent"] = self.metrics.get("sent", 0) + 1
                    await self._remove_persisted_envelope(envelope["id"])
                    await self._log_delivery(envelope, True, receipt, elapsed)
                    sent_count = max(1, self.metrics["sent"])
                    prev_avg = self.metrics.get("avg_delivery_time", 0.0)
                    self.metrics["avg_delivery_time"] = (prev_avg * (sent_count - 1) + elapsed) / sent_count
                else:
                    self.metrics["failed"] = self.metrics.get("failed", 0) + 1
                    envelope["last_error"] = receipt or "unknown_adapter_error"
                    if envelope["attempts"] >= envelope.get("max_attempts", self.max_attempts):
                        # move to dead-letter
                        await self._remove_persisted_envelope(envelope["id"])
                        await self._move_to_dead_letter(envelope)
                        await self._log_delivery(envelope, False, receipt, elapsed, final=True)
                    else:
                        backoff = self.base_backoff * (2 ** (envelope["attempts"] - 1))
                        backoff += random.uniform(0, self.jitter)
                        envelope["next_attempt_at"] = now_ts() + backoff
                        await self._reschedule_envelope(envelope)

                # allow other tasks to run
                await asyncio.sleep(0)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Proactive worker loop encountered exception: %s", traceback.format_exc())
                await asyncio.sleep(1.0)

        logger.debug("Proactive worker exiting")

    async def _deliver_envelope(self, envelope: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Deliver using registered adapter or deliver_hook.
        Supports sync/async adapters and multiple adapter return types.
        """
        channel = envelope.get("channel")
        adapter = self.adapters.get(channel)
        try:
            if adapter:
                # adapter can be object with .send or be a callable function
                send_fn = getattr(adapter, "send", None) or adapter
                try:
                    maybe = send_fn(envelope)
                except TypeError:
                    # try calling adapter as callable with whole envelope
                    maybe = adapter(envelope)
                # unwrap coroutine or sync result
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

            # fallback to deliver_hook (persona.notify or interaction layer)
            dh = self.deliver_hook
            if dh:
                try:
                    # persona.notify(channel=..., message=...)
                    notify_fn = getattr(dh, "notify", None)
                    if notify_fn:
                        try:
                            maybe = notify_fn(channel=envelope["channel"], message=envelope["payload"]["text"])
                        except TypeError:
                            maybe = notify_fn({"channel": envelope["channel"], "message": envelope["payload"]["text"], "meta": envelope.get("payload", {}).get("meta", {})})
                        if asyncio.iscoroutine(maybe):
                            await maybe
                        return True, "persona_notify_called"
                except Exception:
                    logger.debug("deliver_hook persona.notify path failed, try direct call", exc_info=True)

                if callable(dh):
                    try:
                        maybe = dh(envelope)
                        if asyncio.iscoroutine(maybe):
                            await maybe
                        return True, "deliver_hook_called"
                    except Exception as e:
                        logger.exception("deliver_hook callable failed: %s", e)
                        return False, str(e)

            return False, f"no_delivery_path_for_channel:{channel}"
        except Exception as e:
            logger.exception("Delivery adapter raised exception")
            return False, str(e)

    # ---------------------------
    # Persistence: queue, logs, dead-letter
    # ---------------------------
    async def _persist_queue(self, force: bool = False):
        store = getattr(self.core, "store", None) or getattr(self.core, "secure_store", None)
        if not store:
            logger.debug("No store available to persist proactive queue.")
            return
        try:
            payload = {"queue": list(self._persisted_queue), "ts": now_ts()}
            put = getattr(store, "put", None)
            if put:
                try:
                    maybe = put(self.PERSIST_KEY_QUEUE, "proactive", payload)
                except TypeError:
                    maybe = put(self.PERSIST_KEY_QUEUE, payload)
                if asyncio.iscoroutine(maybe):
                    await maybe
            elif hasattr(store, "__setitem__"):
                store[self.PERSIST_KEY_QUEUE] = payload
            self._last_persist = now_ts()
            logger.debug("Proactive queue persisted (count=%d).", len(self._persisted_queue))
        except Exception:
            logger.exception("Failed to persist proactive queue")

    async def _load_persisted_queue(self):
        store = getattr(self.core, "store", None) or getattr(self.core, "secure_store", None)
        if not store:
            logger.debug("No store available to load proactive queue.")
            return
        try:
            get = getattr(store, "get", None)
            if get:
                try:
                    maybe = get(self.PERSIST_KEY_QUEUE)
                except TypeError:
                    maybe = get(self.PERSIST_KEY_QUEUE)
            else:
                maybe = store.get(self.PERSIST_KEY_QUEUE) if hasattr(store, "get") else None

            data = await maybe if asyncio.iscoroutine(maybe) else maybe
            if data and isinstance(data, dict) and "queue" in data:
                async with self._queue_lock:
                    self._persisted_queue = data.get("queue", []) or []
                    self.metrics["queued"] = len(self._persisted_queue)
                    now = now_ts()
                    for env in list(self._persisted_queue):
                        if env.get("next_attempt_at", 0) <= now + 1.0:
                            await self._in_queue.put(env)
                logger.info("Loaded persisted proactive queue (count=%d)", len(self._persisted_queue))
        except Exception:
            logger.exception("Failed to load persisted proactive queue")

    async def _remove_persisted_envelope(self, envelope_id: str):
        async with self._queue_lock:
            before = len(self._persisted_queue)
            self._persisted_queue = [e for e in self._persisted_queue if e.get("id") != envelope_id]
            after = len(self._persisted_queue)
            self.metrics["queued"] = after
            if before != after:
                await self._persist_queue_debounced()

    async def _reschedule_envelope(self, envelope: Dict[str, Any]):
        async with self._queue_lock:
            found = False
            for i, e in enumerate(self._persisted_queue):
                if e.get("id") == envelope.get("id"):
                    self._persisted_queue[i] = envelope
                    found = True
                    break
            if not found:
                self._persisted_queue.append(envelope)
            # keep queue sorted
            self._persisted_queue.sort(key=lambda e: (e.get("next_attempt_at", 0), -e.get("priority", 0)))
            self.metrics["queued"] = len(self._persisted_queue)
            if envelope.get("next_attempt_at", 0) <= now_ts() + 1.0:
                await self._in_queue.put(envelope)
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
    # Dead-letter handling
    # ---------------------------
    async def _move_to_dead_letter(self, envelope: Dict[str, Any]):
        async with self._queue_lock:
            envelope_copy = dict(envelope)
            envelope_copy["moved_to_dead_at"] = now_ts()
            self._dead_letter_queue.append(envelope_copy)
            await self._persist_dead_letter()
        logger.warning("Envelope moved to dead-letter: %s", envelope.get("id"))

    async def _persist_dead_letter(self):
        store = getattr(self.core, "store", None) or getattr(self.core, "secure_store", None)
        if not store:
            return
        try:
            put = getattr(store, "put", None)
            payload = {"dead": list(self._dead_letter_queue), "ts": now_ts()}
            if put:
                try:
                    maybe = put(self.PERSIST_KEY_DEAD, "proactive_dead", payload)
                except TypeError:
                    maybe = put(self.PERSIST_KEY_DEAD, payload)
                if asyncio.iscoroutine(maybe):
                    await maybe
            elif hasattr(store, "__setitem__"):
                store[self.PERSIST_KEY_DEAD] = payload
            logger.debug("Dead-letter persisted (count=%d)", len(self._dead_letter_queue))
        except Exception:
            logger.exception("Failed to persist dead-letter queue")

    async def _load_dead_letter_queue(self):
        store = getattr(self.core, "store", None) or getattr(self.core, "secure_store", None)
        if not store:
            return
        try:
            get = getattr(store, "get", None)
            maybe = None
            if get:
                try:
                    maybe = get(self.PERSIST_KEY_DEAD)
                except TypeError:
                    maybe = get(self.PERSIST_KEY_DEAD)
            else:
                maybe = store.get(self.PERSIST_KEY_DEAD) if hasattr(store, "get") else None
            data = await maybe if asyncio.iscoroutine(maybe) else maybe
            if data and isinstance(data, dict):
                self._dead_letter_queue = data.get("dead", []) or []
                logger.info("Loaded dead-letter queue (count=%d)", len(self._dead_letter_queue))
        except Exception:
            logger.exception("Failed to load dead-letter queue")

    # ---------------------------
    # Logging
    # ---------------------------
    async def _log_delivery(self, envelope: Dict[str, Any], success: bool, receipt: Optional[str], latency: float, final: bool = False):
        entry = {
            "id": f"log_{int(now_ts() * 1000)}_{uuid.uuid4().hex[:6]}",
            "envelope_id": envelope.get("id"),
            "subject_identity": envelope.get("subject_identity"),
            "channel": envelope.get("channel"),
            "success": bool(success),
            "receipt": receipt,
            "latency": latency,
            "attempts": envelope.get("attempts"),
            "final": bool(final),
            "ts": now_ts(),
            "payload_meta": envelope.get("payload", {}),
        }
        async with self._logs_lock:
            self._logs.append(entry)
            if len(self._logs) > 5000:
                self._logs = self._logs[-5000:]
        await self._persist_logs_debounced()

    async def _persist_logs(self):
        store = getattr(self.core, "store", None) or getattr(self.core, "secure_store", None)
        if not store:
            return
        try:
            put = getattr(store, "put", None)
            payload = {"logs": list(self._logs), "ts": now_ts()}
            if put:
                try:
                    maybe = put(self.PERSIST_KEY_LOGS, "proactive_logs", payload)
                except TypeError:
                    maybe = put(self.PERSIST_KEY_LOGS, payload)
                if asyncio.iscoroutine(maybe):
                    await maybe
            elif hasattr(store, "__setitem__"):
                store[self.PERSIST_KEY_LOGS] = payload
            logger.debug("Proactive logs persisted (count=%d)", len(self._logs))
        except Exception:
            logger.exception("Failed to persist proactive logs")

    async def _persist_logs_debounced(self):
        # simple direct write for now — could debounce similarly to queue
        await self._persist_logs()

    # ---------------------------
    # Rate limiting (sliding window)
    # ---------------------------
    def _allow_send(self, channel: str) -> bool:
        now = now_ts()
        window = 60.0
        lst = self._channel_counters.setdefault(channel, [])
        # prune
        lst = [t for t in lst if now - t < window]
        self._channel_counters[channel] = lst
        if len(lst) >= self.rate_limit_per_minute:
            return False
        self._channel_counters[channel].append(now)
        return True

    # ---------------------------
    # Hologram helpers (optional)
    # ---------------------------
    async def _holo_spawn(self, envelope: Dict[str, Any]):
        if not self.hologram_call:
            return
        try:
            nid = f"pmsg_{envelope.get('id')[:8]}"
            maybe = self.hologram_call("spawn_and_link", node_id=nid, node_type="comm", label=f"Proactive:{envelope.get('channel')}", size=3, source_id="ProactiveCommunicator", link_id=f"link_{nid}")
            if asyncio.iscoroutine(maybe):
                await maybe
            maybe2 = self.hologram_call("set_node_active", "ProactiveCommunicator")
            if asyncio.iscoroutine(maybe2):
                await maybe2
        except Exception:
            logger.debug("Hologram spawn failed (non-fatal)")

    async def _holo_idle(self, envelope: Dict[str, Any]):
        if not self.hologram_call:
            return
        try:
            maybe = self.hologram_call("set_node_idle", "ProactiveCommunicator")
            if asyncio.iscoroutine(maybe):
                await maybe
            try:
                maybe2 = self.hologram_call("despawn_and_unlink", None, None)
                if asyncio.iscoroutine(maybe2):
                    await maybe2
            except Exception:
                pass
        except Exception:
            pass

    # ---------------------------
    # Authorization (hardened)
    # ---------------------------
    async def _authorize_enqueue(self, subject_identity: str, channel: str, visibility: str) -> bool:
        """
        Authorize enqueueing of proactive messages.

        Rules (in order):
        1. If subject_identity is owner_primary/owner -> check stored user profile proactive_prefs.
        2. If no profile found, check core flag `allow_proactive_by_default`.
        3. If security_orchestrator present, defer to it.
        4. Default: deny.
        """
        if subject_identity in ("owner_primary", "owner"):
            try:
                prefs = {}
                # try core.save/load profile paths
                if hasattr(self.core, "load_user_profile"):
                    maybe = self.core.load_user_profile()
                    prefs = await maybe if asyncio.iscoroutine(maybe) else (maybe or {})
                elif hasattr(self.core, "persona") and getattr(self.core, "persona", None) is not None:
                    persona_core = getattr(self.core, "persona", None)
                    load_m = getattr(persona_core, "load_user_profile", None)
                    if load_m:
                        maybe = load_m()
                        prefs = await maybe if asyncio.iscoroutine(maybe) else (maybe or {})

                # fallback to store keys
                if not prefs:
                    core_store = getattr(self.core, "store", None) or getattr(self.core, "secure_store", None)
                    if core_store and hasattr(core_store, "get"):
                        for k in ("user_profile", "user_profile_small", "profile", "owner_profile"):
                            try:
                                data = await core_store.get(k) if asyncio.iscoroutinefunction(core_store.get) else core_store.get(k)
                                if data:
                                    prefs = data or {}
                                    break
                            except Exception:
                                continue

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
                                    if start <= nowh < end:
                                        return False
                                else:
                                    if nowh >= start or nowh < end:
                                        return False
                            except Exception:
                                pass
                        return True

                # allow_proactive_by_default override (dev instances)
                try:
                    cfg_flag = False
                    if isinstance(getattr(self.core, "config", None), dict):
                        cfg_flag = bool(self.core.config.get("allow_proactive_by_default", False))
                    if not cfg_flag:
                        cfg_flag = bool(getattr(self.core, "allow_proactive_by_default", False))
                    if cfg_flag:
                        logger.debug("Proactive allowed by core.allow_proactive_by_default fallback (owner path).")
                        return True
                except Exception:
                    pass

                return False
            except Exception:
                return False

        # non-owner: defer to security orchestrator if present
        sec = getattr(self.core, "security_orchestrator", None)
        if sec and hasattr(sec, "authorize_proactive"):
            try:
                resp = sec.authorize_proactive(subject_identity, channel, visibility)
                return await resp if asyncio.iscoroutine(resp) else bool(resp)
            except Exception:
                return False
        return False

    async def enable_proactive_defaults(self, subject_identity: str = "owner_primary", channels: Optional[List[str]] = None, quiet_hours: Optional[Tuple[int,int]] = None):
        """
        Persist simple proactive_prefs for owner so proactive can run.
        """
        channels = channels or ["logging", "console"]
        small = {"proactive_prefs": {"enabled": True, "channels": channels}}
        try:
            if hasattr(self.core, "save_user_profile"):
                maybe = self.core.save_user_profile(small)
                if asyncio.iscoroutine(maybe):
                    await maybe
                logger.info("Wrote proactive prefs via core.save_user_profile")
                return True
            if hasattr(self.core, "persona") and hasattr(self.core.persona, "core") and hasattr(self.core.persona.core, "save_user_profile"):
                maybe = self.core.persona.core.save_user_profile(small)
                if asyncio.iscoroutine(maybe):
                    await maybe
                logger.info("Wrote proactive prefs via persona.core.save_user_profile")
                return True
        except Exception:
            logger.debug("save_user_profile not available or failed; falling back to store")

        store = getattr(self.core, "store", None) or getattr(self.core, "secure_store", None)
        if store and hasattr(store, "put"):
            try:
                key = "user_profile_small"
                maybe = store.put(key, small)
                if asyncio.iscoroutine(maybe):
                    await maybe
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
                    # remove from dead letter
                    self._dead_letter_queue.pop(i)
                    # ensure ordering
                    self._persisted_queue.sort(key=lambda e: (e.get("next_attempt_at", 0), -e.get("priority", 0)))
                    await self._persist_queue()
                    await self._persist_dead_letter()
                    # schedule immediately
                    await self._in_queue.put(env)
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

    def integrate_with_interaction(self, interaction_core: Any):
        """
        Set deliver_hook to integration that calls interaction_core's scheduling or outbound features.
        """
        async def _deliver_via_interaction(envelope: Dict[str, Any]):
            try:
                if hasattr(interaction_core, "schedule_proactive"):
                    await interaction_core.schedule_proactive(channel=envelope.get("channel"), user_id=envelope.get("subject_identity"), content=envelope.get("payload", {}).get("text", ""), at_seconds=0, metadata=envelope.get("payload", {}).get("meta", {}))
                    return True
                if hasattr(interaction_core, "send_outbound"):
                    outcall = getattr(interaction_core, "send_outbound")
                    maybe = outcall({"channel": envelope.get("channel"), "content": envelope.get("payload", {}).get("text", "")})
                    if asyncio.iscoroutine(maybe):
                        await maybe
                    return True
            except Exception:
                logger.exception("Interaction integration failed")
            return False

        self.deliver_hook = _deliver_via_interaction
        logger.info("ProactiveCommunicator integrated with InteractionCore")

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