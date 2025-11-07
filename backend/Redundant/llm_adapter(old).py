"""
llm_adapter.py - Enterprise LLM Adapter for A.A.R.I.A
*** Rewritten to default to a local Llama3 model via Ollama ***

Production Features:
- Exponential backoff retry with intelligent error classification
- High-precision latency monitoring with perf_counter
- Universal response parsing across LLM API formats
- Adaptive timeout scaling based on request characteristics
- Comprehensive health checks with detailed diagnostics
- Professional logging and exception hierarchy
- Request size validation and connection pooling
"""

import os
import time
import logging
import json
from typing import List, Dict, Any
from urllib.parse import urljoin
import requests


# # ==============================================================
# # Logger Setup
# # ==============================================================
# logger = logging.getLogger("AARIA.LLM")
# if not logger.handlers:
#     handler = logging.StreamHandler()
#     handler.setFormatter(logging.Formatter("%(asctime)s [AARIA.LLM] [%(levelname)s] %(message)s"))
#     logger.addHandler(handler)
# logger.setLevel(logging.INFO)
logger = logging.getLogger(__name__)

# ==============================================================
# Exception Hierarchy
# ==============================================================
class LLMAdapterError(Exception):
    """Base exception for LLM adapter operational errors."""
    pass


class LLMConfigurationError(LLMAdapterError):
    """Configuration-related errors."""
    pass


class LLMConnectionError(LLMAdapterError):
    """Network and connection-related errors."""
    pass


# ==============================================================
# LLM Adapter
# ==============================================================
class LLMAdapter:
    """
    Enterprise-grade LLM interface for A.A.R.I.A AI Assistant.
    
    Environment Configuration (Optional Overrides):
      LLM_API_BASE       - Base URL (default: http://localhost:11434/v1 for Ollama)
      LLM_MODEL          - Model name (default: llama3) 
      LLM_TIMEOUT        - Request timeout seconds (default: 120)
      LLM_API_KEY        - Optional API key for authentication
      LLM_MAX_RETRIES    - Maximum retry attempts (default: 3)
      LLM_BACKOFF_FACTOR - Exponential backoff multiplier (default: 0.5)
      LLM_DEBUG_LOGGING  - Enable detailed request/response logging (default: false)
    """

    def __init__(self):
        # Core Configuration - now defaults to a local Llama3 setup
        self.api_base = os.getenv("LLM_API_BASE", "http://localhost:11434/v1").rstrip('/')
        self.model = os.getenv("LLM_MODEL", "llama3:latest")
        self.timeout = int(os.getenv("LLM_TIMEOUT", "300"))
        self.api_key = os.getenv("LLM_API_KEY")
        self.max_retries = int(os.getenv("LLM_MAX_RETRIES", "3"))
        self.backoff = float(os.getenv("LLM_BACKOFF_FACTOR", "0.5"))
        self.enable_debug_logging = os.getenv("LLM_DEBUG_LOGGING", "false").lower() == "true"

        # Validate Configuration
        self._validate_config()

        # Initialize HTTP Session with connection pooling
        self.session = requests.Session()
        self.session.headers.update(self._headers())
        
        # Configure connection pooling for production
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=10,
            max_retries=2
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

        # Build Endpoint URLs
        self._chat_url = urljoin(self.api_base + "/", "chat/completions")
        self._completion_url = urljoin(self.api_base + "/", "completions")
        self._models_url = urljoin(self.api_base + "/", "models")

        logger.info(f"LLMAdapter initialized → {self.api_base} (model: {self.model})")

    # ----------------------------------------------------------
    # Configuration & Validation
    # ----------------------------------------------------------
    def _validate_config(self) -> None:
        """Validate all configuration parameters."""
        if not self.api_base.startswith(("http://", "https://")):
            raise LLMConfigurationError(f"Invalid API base URL: {self.api_base}")
        if self.timeout <= 0:
            raise LLMConfigurationError("Timeout must be positive")
        if self.max_retries < 0:
            raise LLMConfigurationError("Max retries cannot be negative")
        if self.backoff <= 0:
            raise LLMConfigurationError("Backoff factor must be positive")

    def _validate_request_size(self, messages: List[Dict[str, str]], max_tokens: int) -> None:
        """Validate request parameters to prevent API abuse."""
        total_chars = sum(len(m.get("content", "")) for m in messages)
        
        if total_chars > 16000:  # ~4K tokens
            raise LLMConfigurationError(f"Request too large: {total_chars} characters")
        
        if max_tokens > 4000:
            raise LLMConfigurationError(f"Max tokens too high: {max_tokens}")

    def _headers(self) -> Dict[str, str]:
        """Generate request headers with authentication."""
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "AARIA-AI-Assistant/1.0"
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def _calculate_timeout(self, messages: List[Dict[str, str]], max_tokens: int) -> int:
        """
        Calculate adaptive timeout based on request characteristics.
        
        Scales timeout for larger requests to prevent premature failures
        while maintaining reasonable upper bounds.
        """
        base_timeout = self.timeout
        total_chars = sum(len(m.get("content", "")) for m in messages)
        
        # Scale based on request size
        if total_chars > 1000:
            base_timeout += 30
        if max_tokens > 1000:
            base_timeout += 60
            
        return min(base_timeout, 300)  # Cap at 5 minutes

    # ----------------------------------------------------------
    # Response Parsing
    # ----------------------------------------------------------
    def _parse_response(self, data: Dict[str, Any]) -> str:
        """
        Universal response parser supporting multiple LLM API formats.
        
        Handles:
        - OpenAI format: {"choices": [{"message": {"content": "..."}}]}
        - Ollama format: {"response": "..."} 
        - LocalAI format: {"result": "..."}
        - Generic formats with fallback parsing
        """
        # OpenAI-compatible format
        if "choices" in data and isinstance(data["choices"], list) and data["choices"]:
            try:
                message = data["choices"][0].get("message", {})
                content = message.get("content")
                if isinstance(content, str) and content.strip():
                    return content.strip()
            except (KeyError, TypeError, AttributeError):
                pass  # Continue to other formats

        # Direct response fields (common alternatives)
        for key in ("response", "result", "output", "content", "text"):
            value = data.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

        # Nested message structure
        if isinstance(data.get("message"), dict):
            content = data["message"].get("content")
            if isinstance(content, str) and content.strip():
                return content.strip()

        # Depth-limited recursive search as final fallback
        def deep_find_string(obj, depth: int = 0) -> str:
            """Safely search for string content with recursion limit."""
            if depth > 3:  # Prevent infinite recursion
                return None
                
            if isinstance(obj, str) and obj.strip():
                return obj.strip()
                
            if isinstance(obj, dict):
                for value in obj.values():
                    result = deep_find_string(value, depth + 1)
                    if result:
                        return result
                        
            if isinstance(obj, list):
                for item in obj:
                    result = deep_find_string(item, depth + 1)
                    if result:
                        return result
            return None

        fallback_result = deep_find_string(data)
        if fallback_result:
            return fallback_result

        # Log detailed error for debugging
        logger.error(f"Unparseable LLM response structure. Keys: {list(data.keys())}")
        raise LLMAdapterError("Failed to extract meaningful content from LLM response")

    # ----------------------------------------------------------
    # Core Chat Interface
    # ----------------------------------------------------------
    def chat(self, messages: List[Dict[str, str]], max_tokens: int = 512, 
             temperature: float = 0.3) -> str:
        """
        Send chat messages to LLM with robust error handling and retries.
        
        Args:
            messages: List of message dicts with 'role' and 'content'
            max_tokens: Maximum response length in tokens
            temperature: Creativity control (0.0-1.0), clamped to valid range
            
        Returns:
            LLM response as string
            
        Raises:
            LLMAdapterError: After all retries exhausted or fatal error
            LLMConnectionError: For persistent network issues
        """
        # Validate request size first
        self._validate_request_size(messages, max_tokens)

        # Prepare request payload
        payload = {
            "model": self.model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": max(0.0, min(1.0, temperature)),  # Ensure valid range
            "stream": False,
        }

        # Calculate adaptive timeout
        adaptive_timeout = self._calculate_timeout(messages, max_tokens)

        # Debug logging for development
        if self.enable_debug_logging:
            logger.debug(f"LLM Request to {self._chat_url}")
            logger.debug(f"Payload: {json.dumps(payload, indent=2)}")

        # Retry loop with exponential backoff
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                logger.info(f"LLM request attempt {attempt + 1}/{self.max_retries + 1}")
                
                # Execute request with high-precision timing
                start_time = time.perf_counter()
                response = self.session.post(
                    self._chat_url, 
                    json=payload, 
                    timeout=adaptive_timeout
                )
                latency = round(time.perf_counter() - start_time, 3)
                
                logger.info(f"LLM response received in {latency}s (HTTP {response.status_code})")
                
                # Validate HTTP response
                response.raise_for_status()
                
                # Parse and return response
                response_data = response.json()
                result = self._parse_response(response_data)
                
                # Debug logging for response
                if self.enable_debug_logging:
                    logger.debug(f"LLM Response preview: {result[:100]}...")
                
                return result

            except (requests.ConnectionError, requests.Timeout) as e:
                last_exception = LLMConnectionError(f"Network error: {e}")
                if attempt < self.max_retries:
                    delay = self.backoff * (2 ** attempt)
                    logger.warning(f"Network issue, retrying in {delay:.1f}s...")
                    time.sleep(delay)
                    continue
                raise last_exception

            except requests.HTTPError as e:
                status_code = e.response.status_code
                last_exception = LLMAdapterError(f"HTTP {status_code}: {e.response.text[:200]}")
                
                # Retry only on server errors (5xx)
                if status_code >= 500 and attempt < self.max_retries:
                    delay = self.backoff * (2 ** attempt)
                    logger.warning(f"Server error {status_code}, retrying in {delay:.1f}s...")
                    time.sleep(delay)
                    continue
                raise last_exception

            except json.JSONDecodeError as e:
                last_exception = LLMAdapterError("Invalid JSON in LLM response")
                raise last_exception

            except Exception as e:
                last_exception = LLMAdapterError(f"Unexpected LLM error: {e}")
                logger.exception(f"Unexpected error during LLM chat (attempt {attempt + 1})")
                if attempt < self.max_retries:
                    delay = self.backoff * (2 ** attempt)
                    time.sleep(delay)
                    continue
                raise last_exception

        # All retries exhausted
        raise LLMAdapterError(f"All {self.max_retries} retries failed") from last_exception

    def completion(self, prompt: str, max_tokens: int = 256, temperature: float = 0.3) -> str:
        """
        Simplified completion interface for single prompts.
        
        Args:
            prompt: Input text for the LLM
            max_tokens: Maximum response length
            temperature: Creativity control
            
        Returns:
            LLM response as string
        """
        return self.chat(
            [{"role": "user", "content": prompt}], 
            max_tokens=max_tokens, 
            temperature=temperature
        )

    # ----------------------------------------------------------
    # System Management & Monitoring
    # ----------------------------------------------------------
    def health_check(self, timeout: int = 5) -> Dict[str, Any]:
        """
        Comprehensive health check with detailed diagnostics.
        
        Returns:
            Dictionary with health status and metrics
        """
        try:
            start_time = time.perf_counter()
            response = self.session.get(self._models_url, timeout=timeout)
            latency = round(time.perf_counter() - start_time, 3)
            
            # Verify configured model is available
            models_available = self.get_available_models()
            model_available = self.model in models_available if models_available else True
            
            status = "healthy" if (response.status_code == 200 and model_available) else "degraded"
            
            result = {
                "status": status,
                "latency_seconds": latency,
                "http_status": response.status_code,
                "reachable": True,
                "model": self.model,
                "model_available": model_available,
                "available_models_count": len(models_available),
                "timestamp": time.time(),
            }
            
            if status == "healthy":
                logger.info(f"LLM Health Check OK ({latency}s)")
            else:
                logger.warning(f"LLM Health Check Degraded (HTTP {response.status_code}, model_available: {model_available})")
                
            return result

        except Exception as e:
            logger.warning(f"LLM health check failed: {e}")
            return {
                "status": "unhealthy",
                "reachable": False,
                "error": str(e),
                "model": self.model,
                "timestamp": time.time(),
            }

    def get_available_models(self) -> List[str]:
        """
        Discover available models from the LLM endpoint.
        
        Returns:
            List of model names/IDs
        """
        try:
            response = self.session.get(self._models_url, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            
            # Handle different response formats
            if "data" in data and isinstance(data["data"], list):  # OpenAI format
                return [model["id"] for model in data["data"] if "id" in model]
            elif "models" in data and isinstance(data["models"], list):  # Ollama format
                return [model["name"] for model in data["models"] if "name" in model]
            else:
                logger.warning(f"Unknown models response format: {list(data.keys())}")
                return []
                
        except Exception as e:
            logger.warning(f"Model discovery failed: {e}")
            return []

    # ----------------------------------------------------------
    # Resource Management
    # ----------------------------------------------------------
    def close(self) -> None:
        """Clean up resources and close HTTP session."""
        if hasattr(self, "session"):
            self.session.close()
        logger.info("LLMAdapter closed cleanly")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# ==============================================================
# Self-Test & Demonstration
# ==============================================================
if __name__ == "__main__":
    print("=== AARIA LLM Adapter Self-Test (Defaulting to Llama3) ===\n")
    
    with LLMAdapter() as llm:
        # Health Check
        health = llm.health_check()
        print(f"Health Status: {health['status']}")
        print(f"Latency: {health.get('latency_seconds', 'N/A')}s")
        print(f"HTTP Status: {health.get('http_status', 'N/A')}")
        print(f"Model Available: {health.get('model_available', 'N/A')}")
        
        # Model Discovery
        models = llm.get_available_models()
        print(f"Available Models: {models}")
        
        # Test Chat Interaction
        try:
            print(f"\nTesting chat with model: {llm.model}")
            response = llm.chat([
                {"role": "system", "content": "You are AARIA, a helpful and confident AI assistant."},
                {"role": "user", "content": "Please introduce yourself briefly and tell me your core capabilities."}
            ], max_tokens=200, temperature=0.7)
            
            print(f"\nResponse:\n{response}")
            print(f"\nResponse Length: {len(response)} characters")
            
        except LLMAdapterError as e:
            print(f"LLM Error: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")
    
    print("\n=== Self-Test Complete ===")