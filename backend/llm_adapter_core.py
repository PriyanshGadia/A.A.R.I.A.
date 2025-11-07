# llm_adapter_core.py
"""
A.A.R.I.A LLM Adapter Core - Enterprise-Grade Multi-Provider Orchestration
Single-file corrected implementation for use with PersonaCore.
"""

from __future__ import annotations
import os
import time
import asyncio
import logging
import json
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, AsyncGenerator
from enum import Enum
from dataclasses import dataclass, field
from contextlib import asynccontextmanager

import aiohttp
import backoff

# Pydantic v2
from pydantic import BaseModel, Field, field_validator

# Prometheus optional - provide safe dummy fallbacks
try:
    from prometheus_client import Counter, Histogram, Gauge
    PROMETHEUS_AVAILABLE = True
except Exception:
    PROMETHEUS_AVAILABLE = False

    class _DummyMetric:
        def __init__(self, *args, **kwargs): pass
        def labels(self, *a, **k): return self
        def inc(self, *a, **k): pass
        def set(self, *a, **k): pass
        def time(self): return self
        def __enter__(self): return self
        def __exit__(self, exc_type, exc, tb): return False

    Counter = Histogram = Gauge = _DummyMetric

# Prometheus metrics (or no-op dummies)
llm_requests_total = Counter('llm_requests_total', 'Total LLM requests', ['provider', 'model'])
llm_request_duration = Histogram('llm_request_duration_seconds', 'LLM request duration', ['provider', 'model'])
llm_tokens_used = Counter('llm_tokens_used', 'Tokens used', ['provider', 'model', 'type'])
llm_errors_total = Counter('llm_errors_total', 'LLM errors', ['provider', 'model', 'error_type'])
llm_health_status = Gauge('llm_health_status', 'LLM health status', ['provider', 'model'])
llm_circuit_breaker_state = Gauge('llm_circuit_breaker_state', 'Circuit breaker state', ['provider', 'model'])

# Logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [AARIA.LLM] [%(levelname)s] [%(name)s] %(message)s'
)
logger = logging.getLogger("AARIA.LLM.Core")

# ----- Types & Models -----
class LLMProvider(str, Enum):
    OLLAMA = "ollama"
    GROQ = "groq"
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    AZURE = "azure"

class LLMRequest(BaseModel):
    messages: List[Dict[str, str]] = Field(..., description="Chat messages")
    max_tokens: int = Field(default=2048, ge=1, le=128000)
    temperature: float = Field(default=0.3, ge=0.0, le=2.0)
    stream: bool = Field(default=False)

    @field_validator("messages")
    @classmethod
    def _validate_messages(cls, v):
        if not v:
            raise ValueError("Messages list cannot be empty")
        for msg in v:
            if not isinstance(msg, dict) or "role" not in msg or "content" not in msg:
                raise ValueError("Each message must be a dict with 'role' and 'content'")
        return v

class LLMResponse(BaseModel):
    content: str
    model: str
    provider: LLMProvider
    tokens_used: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    latency: float = 0.0
    finish_reason: Optional[str] = None

class HealthStatus(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"

class HealthCheckResult(BaseModel):
    status: HealthStatus
    provider: LLMProvider
    latency: float
    model_available: bool
    available_models: List[str] = []
    details: Dict[str, Any] = {}

@dataclass
class AdapterConfig:
    provider: LLMProvider
    api_base: str
    api_key: Optional[str]
    model: str
    timeout: int = 120
    max_retries: int = 3
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: int = 60

# ----- Exceptions -----
class LLMError(Exception): pass
class LLMConfigurationError(LLMError): pass
class LLMConnectionError(LLMError): pass
class LLMRateLimitError(LLMError): pass
class LLMContextLengthError(LLMError): pass
class LLMCircuitBreakerOpenError(LLMError): pass

# ----- Circuit Breaker -----
class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60):
        self.failure_threshold = int(failure_threshold)
        self.recovery_timeout = int(recovery_timeout)
        self.failures = 0
        self.last_failure_time = 0.0
        self.state = "CLOSED"

    def can_execute(self) -> bool:
        if self.state == "OPEN":
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = "HALF_OPEN"
                return True
            return False
        return True

    def on_success(self):
        self.failures = 0
        if self.state == "HALF_OPEN":
            self.state = "CLOSED"

    def on_failure(self):
        self.failures += 1
        self.last_failure_time = time.time()
        if self.failures >= self.failure_threshold:
            self.state = "OPEN"

    def get_state(self) -> str:
        return self.state

# ----- Base Adapter -----
class BaseLLMAdapter(ABC):
    def __init__(self, config: AdapterConfig):
        self.config = config
        self.logger = logging.getLogger(f"AARIA.LLM.{config.provider.value}")
        self.session: Optional[aiohttp.ClientSession] = None
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=config.circuit_breaker_threshold,
            recovery_timeout=config.circuit_breaker_timeout
        )
        self._validate_config()

    def _validate_config(self) -> None:
        if not isinstance(self.config.api_base, str) or (self.config.api_base and not self.config.api_base.startswith(("http://", "https://"))):
            raise LLMConfigurationError(f"Invalid API base URL: {self.config.api_base!r}")
        if self.config.timeout <= 0:
            raise LLMConfigurationError("Timeout must be positive")
        if self.config.max_retries < 0:
            raise LLMConfigurationError("Max retries cannot be negative")
        if not (isinstance(self.config.model, str) and self.config.model.strip()):
            raise LLMConfigurationError("Model name cannot be empty")
        if self.config.circuit_breaker_threshold < 1:
            raise LLMConfigurationError("Circuit breaker threshold must be at least 1")

    async def __aenter__(self):
        if self.session is None:
            # optimized session
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.config.timeout),
                connector=aiohttp.TCPConnector(limit=100, limit_per_host=20, keepalive_timeout=30),
                headers=self._get_headers()
            )
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.session:
            await self.session.close()
            self.session = None

    @abstractmethod
    def _get_headers(self) -> Dict[str, str]:
        pass

    @abstractmethod
    def _build_payload(self, request: LLMRequest) -> Dict[str, Any]:
        pass

    @abstractmethod
    def _parse_response(self, data: Dict[str, Any]) -> LLMResponse:
        pass

    @abstractmethod
    def _parse_stream_chunk(self, chunk: bytes) -> Optional[str]:
        pass

    def _update_circuit_breaker_metrics(self):
        state_value = {"CLOSED": 0, "HALF_OPEN": 1, "OPEN": 2}.get(self.circuit_breaker.get_state(), 0)
        try:
            llm_circuit_breaker_state.labels(provider=self.config.provider.value, model=self.config.model).set(state_value)
        except Exception:
            pass

    # backoff: retry on connection and rate limit errors
    @backoff.on_exception(backoff.expo, (LLMConnectionError, LLMRateLimitError), max_tries=3, max_time=60)
    async def chat(self, request: LLMRequest) -> LLMResponse:
        if not self.circuit_breaker.can_execute():
            llm_errors_total.labels(provider=self.config.provider.value, model=self.config.model, error_type="circuit_breaker").inc()
            raise LLMCircuitBreakerOpenError("Circuit breaker is OPEN; failing fast.")

        if request.stream:
            raise ValueError("Use stream_chat for streaming responses")

        start = time.perf_counter()
        try:
            payload = self._build_payload(request)
            # Ensure session available
            if self.session is None:
                await self.__aenter__()

            # metrics timer
            timer = llm_request_duration.labels(provider=self.config.provider.value, model=self.config.model).time()
            try:
                async with self.session.post(self._get_chat_url(), json=payload) as resp:
                    if resp.status == 429:
                        llm_errors_total.labels(provider=self.config.provider.value, model=self.config.model, error_type="rate_limit").inc()
                        self.circuit_breaker.on_failure()
                        raise LLMRateLimitError("Rate limited")
                    resp.raise_for_status()
                    data = await resp.json()
            finally:
                try:
                    timer.__exit__(None, None, None)
                except Exception:
                    pass

            llm_resp = self._parse_response(data)
            llm_resp.latency = time.perf_counter() - start

            # update metrics
            llm_requests_total.labels(provider=self.config.provider.value, model=self.config.model, status="success").inc()
            if llm_resp.input_tokens > 0:
                llm_tokens_used.labels(provider=self.config.provider.value, model=self.config.model, type="input").inc(llm_resp.input_tokens)
            if llm_resp.output_tokens > 0:
                llm_tokens_used.labels(provider=self.config.provider.value, model=self.config.model, type="output").inc(llm_resp.output_tokens)
            if llm_resp.tokens_used > 0:
                llm_tokens_used.labels(provider=self.config.provider.value, model=self.config.model, type="total").inc(llm_resp.tokens_used)

            self.circuit_breaker.on_success()
            self._update_circuit_breaker_metrics()
            self.logger.info(f"LLM success provider={self.config.provider.value} model={self.config.model} latency={llm_resp.latency:.3f}s tokens={llm_resp.tokens_used}")
            return llm_resp

        except aiohttp.ClientResponseError as e:
            latency = time.perf_counter() - start
            llm_errors_total.labels(provider=self.config.provider.value, model=self.config.model, error_type="http").inc()
            self.circuit_breaker.on_failure()
            self._update_circuit_breaker_metrics()
            raise LLMConnectionError(f"HTTP error after {latency:.2f}s: {e}") from e
        except aiohttp.ClientError as e:
            latency = time.perf_counter() - start
            llm_errors_total.labels(provider=self.config.provider.value, model=self.config.model, error_type="connection").inc()
            self.circuit_breaker.on_failure()
            self._update_circuit_breaker_metrics()
            raise LLMConnectionError(f"Network error after {latency:.2f}s: {e}") from e
        except asyncio.TimeoutError as e:
            latency = time.perf_counter() - start
            llm_errors_total.labels(provider=self.config.provider.value, model=self.config.model, error_type="timeout").inc()
            self.circuit_breaker.on_failure()
            self._update_circuit_breaker_metrics()
            raise LLMConnectionError(f"Timeout after {latency:.2f}s") from e
        except LLMRateLimitError:
            raise
        except Exception as e:
            latency = time.perf_counter() - start
            llm_requests_total.labels(provider=self.config.provider.value, model=self.config.model, status="failure").inc()
            llm_errors_total.labels(provider=self.config.provider.value, model=self.config.model, error_type="other").inc()
            self.circuit_breaker.on_failure()
            self._update_circuit_breaker_metrics()
            self.logger.exception("Unexpected adapter error")
            raise LLMError(f"Adapter error after {latency:.2f}s: {e}") from e

    async def stream_chat(self, request: LLMRequest) -> AsyncGenerator[str, None]:
        if not self.circuit_breaker.can_execute():
            raise LLMCircuitBreakerOpenError("Circuit breaker is OPEN; fail fast.")

        request.stream = True
        payload = self._build_payload(request)
        if self.session is None:
            await self.__aenter__()

        try:
            async with self.session.post(self._get_chat_url(), json=payload) as resp:
                resp.raise_for_status()
                async for chunk in resp.content.iter_chunked(1024):
                    if not chunk:
                        continue
                    text = self._parse_stream_chunk(chunk)
                    if text:
                        yield text
            self.circuit_breaker.on_success()
            self._update_circuit_breaker_metrics()
        except aiohttp.ClientError as e:
            self.circuit_breaker.on_failure()
            self._update_circuit_breaker_metrics()
            self.logger.error("Streaming connection error", exc_info=e)
            raise LLMConnectionError(f"Streaming connection error: {e}") from e
        except Exception as e:
            self.circuit_breaker.on_failure()
            self._update_circuit_breaker_metrics()
            self.logger.error("Streaming unexpected error", exc_info=e)
            raise LLMError(f"Streaming error: {e}") from e

    async def health_check(self) -> HealthCheckResult:
        start = time.perf_counter()
        details = {"reachable": False}
        try:
            if self.session is None:
                await self.__aenter__()
            async with self.session.get(self._get_models_url()) as resp:
                latency = time.perf_counter() - start
                details["http_status"] = resp.status
                details["connectivity_latency"] = latency
                details["reachable"] = True
                if resp.status == 200:
                    available = await self._get_available_models(resp)
                    model_ok = self.config.model in available
                    gen_ok = await self._test_generation()
                    threshold = 3.0
                    latency_ok = latency <= threshold
                    if model_ok and gen_ok and latency_ok:
                        status = HealthStatus.HEALTHY
                    elif model_ok and gen_ok:
                        status = HealthStatus.DEGRADED
                        details["warning"] = f"Latency {latency:.2f}s"
                    else:
                        status = HealthStatus.UNHEALTHY
                        details["error"] = "Model missing or generation failed"
                else:
                    status = HealthStatus.UNHEALTHY
                    model_ok = False
                    available = []
                    details["error"] = f"HTTP {resp.status}"
        except Exception as e:
            latency = time.perf_counter() - start
            status = HealthStatus.UNHEALTHY
            model_ok = False
            available = []
            details["error"] = str(e)
            details["connectivity_latency"] = latency

        try:
            health_val = 1.0 if status == HealthStatus.HEALTHY else 0.5 if status == HealthStatus.DEGRADED else 0.0
            llm_health_status.labels(provider=self.config.provider.value, model=self.config.model).set(health_val)
        except Exception:
            pass

        return HealthCheckResult(status=status, provider=self.config.provider, latency=details.get("connectivity_latency", 0.0), model_available=model_ok, available_models=available, details=details)

    async def _test_generation(self) -> bool:
        try:
            req = LLMRequest(messages=[{"role": "user", "content": "OK"}], max_tokens=4, temperature=0.0)
            resp = await self.chat(req)
            return resp.content.strip().upper() in ("OK", "OK.")
        except Exception:
            return False

    async def _get_available_models(self, resp: aiohttp.ClientResponse) -> List[str]:
        try:
            data = await resp.json()
            if isinstance(data, dict):
                if "data" in data and isinstance(data["data"], list):
                    return [m.get("id") for m in data["data"] if isinstance(m, dict) and "id" in m]
                if "models" in data and isinstance(data["models"], list):
                    # Ollama/Groq-ish
                    return [m.get("name") for m in data["models"] if isinstance(m, dict) and "name" in m]
            return []
        except Exception as e:
            self.logger.debug("Failed to parse models response", exc_info=e)
            return []

    def _get_chat_url(self) -> str:
        # default OpenAI-compatible
        base = self.config.api_base.rstrip("/")
        if self.config.provider == LLMProvider.GROQ:
            return base + "/chat/completions"
        if self.config.provider == LLMProvider.ANTHROPIC:
            return base + "/messages"
        return base + "/chat/completions"

    def _get_models_url(self) -> str:
        base = self.config.api_base.rstrip("/")
        return base + "/models"

    def get_circuit_breaker_state(self) -> str:
        return self.circuit_breaker.get_state()

# ----- Concrete Adapters -----
class OpenAICompatibleAdapter(BaseLLMAdapter):
    def _get_headers(self) -> Dict[str, str]:
        return {"Content-Type": "application/json", "Authorization": f"Bearer {self.config.api_key}", "User-Agent": "AARIA-Orchestration/2.0"}

    def _build_payload(self, request: LLMRequest) -> Dict[str, Any]:
        return {"model": self.config.model, "messages": request.messages, "max_tokens": request.max_tokens, "temperature": request.temperature, "stream": request.stream}

    def _parse_response(self, data: Dict[str, Any]) -> LLMResponse:
        choice = data.get("choices", [{}])[0]
        usage = data.get("usage", {}) or {}
        content = ""
        if "message" in choice and isinstance(choice["message"], dict):
            content = choice["message"].get("content", "")
        elif "text" in choice:
            content = choice.get("text", "")
        return LLMResponse(content=content.strip(), model=data.get("model", self.config.model), provider=self.config.provider, tokens_used=int(usage.get("total_tokens", 0)), input_tokens=int(usage.get("prompt_tokens", 0)), output_tokens=int(usage.get("completion_tokens", 0)), latency=0.0, finish_reason=choice.get("finish_reason"))

    def _parse_stream_chunk(self, chunk: bytes) -> Optional[str]:
        try:
            txt = chunk.decode("utf-8").strip()
            # handle OpenAI-style "data: {json}\n\n"
            if txt.startswith("data:"):
                payload = txt[len("data:"):].strip()
                if payload == "[DONE]":
                    return None
                data = json.loads(payload)
                delta = data.get("choices", [{}])[0].get("delta", {})
                return delta.get("content")
            # fallback raw chunk
            return txt
        except Exception:
            return None

class AnthropicAdapter(BaseLLMAdapter):
    def _get_headers(self) -> Dict[str, str]:
        return {"Content-Type": "application/json", "x-api-key": self.config.api_key, "anthropic-version": "2023-06-01", "User-Agent": "AARIA-Orchestration/2.0"}

    def _build_payload(self, request: LLMRequest) -> Dict[str, Any]:
        system = [m["content"] for m in request.messages if m["role"] == "system"]
        conv = [m for m in request.messages if m["role"] != "system"]
        payload = {"model": self.config.model, "messages": conv, "max_tokens": request.max_tokens, "temperature": request.temperature, "stream": request.stream}
        if system:
            payload["system"] = "\n".join(system)
        return payload

    def _parse_response(self, data: Dict[str, Any]) -> LLMResponse:
        usage = data.get("usage", {}) or {}
        # Anthropic shapes vary; be defensive
        content = ""
        if isinstance(data.get("content"), list) and data["content"]:
            content = data["content"][0].get("text", "")
        elif isinstance(data.get("completion"), str):
            content = data["completion"]
        return LLMResponse(content=content.strip(), model=data.get("model", self.config.model), provider=self.config.provider, tokens_used=int(usage.get("input_tokens", 0)) + int(usage.get("output_tokens", 0)), input_tokens=int(usage.get("input_tokens", 0)), output_tokens=int(usage.get("output_tokens", 0)), latency=0.0, finish_reason=data.get("stop_reason"))

    def _parse_stream_chunk(self, chunk: bytes) -> Optional[str]:
        try:
            txt = chunk.decode("utf-8").strip()
            if txt.startswith("data:"):
                payload = txt[len("data:"):].strip()
                data = json.loads(payload)
                if data.get("type") == "content_block_delta":
                    return data.get("delta", {}).get("text")
            return txt
        except Exception:
            return None

class AzureOpenAIAdapter(OpenAICompatibleAdapter):
    def _get_headers(self) -> Dict[str, str]:
        return {"Content-Type": "application/json", "api-key": self.config.api_key, "User-Agent": "AARIA-Orchestration/2.0"}

# ----- Factory -----
class LLMAdapterFactory:
    _instances: Dict[str, BaseLLMAdapter] = {}
    _adapter_map = {
        LLMProvider.OPENAI: OpenAICompatibleAdapter,
        LLMProvider.GROQ: OpenAICompatibleAdapter,
        LLMProvider.OLLAMA: OpenAICompatibleAdapter,
        LLMProvider.ANTHROPIC: AnthropicAdapter,
        LLMProvider.AZURE: AzureOpenAIAdapter
    }

    @classmethod
    async def create_adapter(cls, provider: LLMProvider, **kwargs) -> BaseLLMAdapter:
        instance_key = f"{provider.value}:{kwargs.get('model','default')}"
        if instance_key not in cls._instances:
            config = cls._create_config(provider, **kwargs)
            adapter_cls = cls._adapter_map.get(provider)
            if not adapter_cls:
                raise LLMConfigurationError(f"No adapter for provider {provider}")
            adapter = adapter_cls(config)
            cls._instances[instance_key] = adapter
        return cls._instances[instance_key]

    @classmethod
    @asynccontextmanager
    async def get_adapter(cls, provider: LLMProvider, **kwargs):
        adapter = await cls.create_adapter(provider, **kwargs)
        async with adapter:
            yield adapter

    @staticmethod
    def _create_config(provider: LLMProvider, **kwargs) -> AdapterConfig:
        env_prefix = provider.value.upper()
        api_base_defaults = {
            "OPENAI": "https://api.openai.com/v1",
            "GROQ": "https://api.groq.com/openai/v1",
            "ANTHROPIC": "https://api.anthropic.com/v1",
            "OLLAMA": "http://localhost:11434/v1",
            "AZURE": ""
        }
        model_defaults = {
            "OPENAI": "gpt-4o",
            "GROQ": "llama-3.1-8b-instant",
            "ANTHROPIC": "claude-3-100k",
            "OLLAMA": "llama3:latest",
            "AZURE": ""
        }
        api_base = kwargs.get("api_base") or os.getenv(f"{env_prefix}_API_BASE") or api_base_defaults.get(env_prefix, "")
        api_key = kwargs.get("api_key") or os.getenv(f"{env_prefix}_API_KEY")
        model = kwargs.get("model") or os.getenv(f"{env_prefix}_MODEL") or model_defaults.get(env_prefix, "")
        if provider != LLMProvider.OLLAMA and not api_key:
            raise LLMConfigurationError(f"{env_prefix}_API_KEY is required for provider {provider.value}")
        return AdapterConfig(provider=provider, api_base=api_base.rstrip("/"), api_key=api_key, model=model, timeout=int(kwargs.get("timeout") or int(os.getenv("LLM_TIMEOUT", "180"))), max_retries=int(kwargs.get("max_retries") or int(os.getenv("LLM_MAX_RETRIES","3"))), circuit_breaker_threshold=int(kwargs.get("circuit_breaker_threshold") or int(os.getenv("LLM_CIRCUIT_BREAKER_THRESHOLD","5"))), circuit_breaker_timeout=int(kwargs.get("circuit_breaker_timeout") or int(os.getenv("LLM_CIRCUIT_BREAKER_TIMEOUT","60"))))

    @classmethod
    async def cleanup(cls):
        for adapter in list(cls._instances.values()):
            try:
                if adapter.session:
                    await adapter.session.close()
            except Exception:
                pass
        cls._instances.clear()

# Small helper/demo for quick tests (non-blocking; can be removed)
async def _demo_quick_check(provider: LLMProvider = LLMProvider.GROQ):
    try:
        async with LLMAdapterFactory.get_adapter(provider) as adapter:
            health = await adapter.health_check()
            logger.info(f"Health: {health.status} latency={health.latency}")
            if health.status != HealthStatus.UNHEALTHY:
                req = LLMRequest(messages=[{"role":"user","content":"Say OK"}], max_tokens=5)
                resp = await adapter.chat(req)
                logger.info(f"Resp: {resp.content[:80]}")
    except LLMConfigurationError as e:
        logger.warning(f"Configuration problem: {e}")
    except Exception as e:
        logger.exception("Adapter demo failed", exc_info=e)