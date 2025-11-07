"""
stubs/llm_adapter_stub.py

Safe local stub adapter for development and CI.
Exports: create_stub_adapter(model: str = None, **kwargs) -> adapter instance

This stub implements the minimal interface expected by your llm_adapter factory and
the rest of the system:
- async context management: __aenter__, __aexit__
- async chat(request: LLMRequest) -> LLMResponse
- async stream_chat(request: LLMRequest) -> async generator of str
- async health_check() -> HealthCheckResult-like dict/object
- get_circuit_breaker_state() -> str
- session attribute (None)
"""

import asyncio
import time
import logging
from typing import Optional, AsyncGenerator, Any, Dict, List

# We import a few types from the main llm_adapter if available for compatibility,
# but this stub is tolerant if types are not present (keeps working).
try:
    from llm_adapter import LLMRequest, LLMResponse, HealthCheckResult, HealthStatus, LLMProvider
    AdapterTypesAvailable = True
except Exception:
    AdapterTypesAvailable = False

logger = logging.getLogger("AARIA.StubAdapter")
logger.setLevel(logging.INFO)

class StubAdapter:
    """
    Lightweight adapter used for development.
    Produces deterministic responses and simulates token usage/latency.
    """

    def __init__(self, model: Optional[str] = None, **kwargs):
        self.model = model or "stub-model-0.1"
        # Expose a config-like surface for compatibility
        self.config = type("C", (), {"model": self.model, "provider": getattr(LLMProvider, "OLLAMA", "ollama")})
        self.session = None
        self._closed = False
        self._circuit_state = "CLOSED"

    # Async context manager compatibility
    async def __aenter__(self):
        # simulate lightweight init
        await asyncio.sleep(0.01)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await asyncio.sleep(0.01)
        self._closed = True

    async def chat(self, request) -> Any:
        """
        Simulate a chat completion.
        Accepts either a Pydantic LLMRequest or a plain object/dict with 'messages' and other fields.
        Returns either a LLMResponse instance (if available) or a simple dict.
        """
        # Simulate latency based on message length
        try:
            messages = getattr(request, "messages", request.get("messages", []))
        except Exception:
            messages = []
        total_chars = sum(len(m.get("content", "")) if isinstance(m, dict) else len(str(m)) for m in messages)
        simulated_latency = min(0.1 + total_chars / 2000.0, 1.5)  # 100ms base, grows with content
        await asyncio.sleep(simulated_latency)

        # Build deterministic (safe) response
        content = " ".join(
            (m.get("content", "") if isinstance(m, dict) else str(m))
            for m in (messages[-1:] or [])
        ).strip()
        if not content:
            content = "Hello from A.A.R.I.A (stub)."

        # Tokens heuristic
        input_tokens = max(1, int(total_chars / 4))
        output_tokens = max(1, min(200, int(len(content) / 4) + 3))
        total_tokens = input_tokens + output_tokens

        # Respect simple max_tokens if present
        max_tokens = getattr(request, "max_tokens", None) or (request.get("max_tokens") if isinstance(request, dict) else None)
        if max_tokens:
            output_tokens = min(output_tokens, int(max_tokens))
            total_tokens = input_tokens + output_tokens

        # Build response object compatible with main code
        if AdapterTypesAvailable:
            resp = LLMResponse(
                content=content,
                model=self.model,
                provider=self.config.provider,
                tokens_used=total_tokens,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                latency=simulated_latency,
                finish_reason="stub"
            )
            return resp
        else:
            return {
                "content": content,
                "model": self.model,
                "provider": getattr(self.config, "provider", "ollama"),
                "tokens_used": total_tokens,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "latency": simulated_latency,
                "finish_reason": "stub"
            }

    async def stream_chat(self, request) -> AsyncGenerator[str, None]:
        """
        Simulate a streaming response by yielding small chunks with tiny delays.
        """
        # Produce tokens in small chunks
        final = (await self.chat(request))
        # If real LLMResponse returned, extract content; else dict
        content = final.content if hasattr(final, "content") else final.get("content", str(final))
        # chunk by words
        words = content.split()
        for w in words:
            await asyncio.sleep(0.02)
            yield w + " "
        # final small pause
        await asyncio.sleep(0.02)

    async def health_check(self) -> Any:
        """
        Return a HealthCheckResult-like object/dict indicating healthy status.
        """
        # Simulate quick health success
        await asyncio.sleep(0.02)
        details = {"reachable": True, "note": "stub adapter responding", "model": self.model}
        if AdapterTypesAvailable:
            # Build HealthCheckResult if type available
            return HealthCheckResult(
                status=HealthStatus.HEALTHY,
                provider=self.config.provider,
                latency=0.02,
                model_available=True,
                available_models=[self.model],
                details=details
            )
        else:
            return {
                "status": "healthy",
                "provider": getattr(self.config, "provider", "ollama"),
                "latency": 0.02,
                "model_available": True,
                "available_models": [self.model],
                "details": details
            }

    def get_circuit_breaker_state(self) -> str:
        return self._circuit_state

# Factory function used by the llm_adapter shim/factory
def create_stub_adapter(model: Optional[str] = None, **kwargs) -> StubAdapter:
    """
    Returns a ready-to-use stub adapter instance.
    Example:
        adapter = create_stub_adapter(model="local-test")
        async with adapter:
            resp = await adapter.chat(request)
    """
    return StubAdapter(model=model, **kwargs)