# console_adapter.py
import asyncio
import time
import logging
from typing import Dict, Any, Tuple

logger = logging.getLogger("AARIA.ConsoleAdapter")

class ConsoleAdapter:
    """
    Minimal adapter for local development. send(envelope) -> (ok, receipt)
    """

    def __init__(self, name: str = "console"):
        self.name = name

    def send(self, envelope: Dict[str, Any]) -> Tuple[bool, str]:
        # synchronous path
        try:
            ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            subj = envelope.get("subject_identity")
            channel = envelope.get("channel")
            payload = envelope.get("payload", {})
            text = payload.get("text", "<no-text>")
            out = f"[{ts}] [ConsoleAdapter:{self.name}] -> to={subj} channel={channel} msg={text}"
            print(out, flush=True)
            logger.info(out)
            return True, f"console_receipt_{int(time.time()*1000)}"
        except Exception as e:
            logger.exception("ConsoleAdapter failed to send")
            return False, str(e)

    async def send_async(self, envelope: Dict[str, Any]) -> Tuple[bool, str]:
        await asyncio.sleep(0.01)
        return self.send(envelope)