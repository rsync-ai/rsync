import os
import random
import time
import requests
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# Connect timeout 5s, read timeout configurable via env (default 120s for slow LLMs)
_CONNECT_TIMEOUT = 5
_READ_TIMEOUT = int(os.getenv("LLM_REQUEST_TIMEOUT_SECONDS", "120"))
_MAX_RETRIES = int(os.getenv("LLM_REQUEST_MAX_RETRIES", "3"))
_RETRY_BASE_SECONDS = float(os.getenv("LLM_REQUEST_RETRY_BASE_SECONDS", "1.0"))
_RETRY_MAX_SECONDS = float(os.getenv("LLM_REQUEST_RETRY_MAX_SECONDS", "30.0"))

# Status codes that warrant a retry. 408/425/429/5xx are transient.
_RETRIABLE_STATUSES = {408, 425, 429, 500, 502, 503, 504}


def _retry_delay(attempt: int) -> float:
    """Full-jitter exponential backoff."""
    exponential = min(_RETRY_BASE_SECONDS * (2 ** attempt), _RETRY_MAX_SECONDS)
    return random.uniform(0, exponential)


class LLMClient:
    """
    Client for interacting with the internal LLM Service.
    """

    def __init__(self):
        self.base_url = os.getenv("LLM_GATEWAY_URL", "http://localhost:5001")

    def complete(
        self,
        prompt_name: str,
        variables: Dict[str, Any],
        model_override: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Execute a prompt via the LLM Gateway.

        Retries on transient errors (timeouts, 5xx, 429) with full-jitter
        exponential backoff. Honours ``Retry-After`` when the server sets it.
        """
        url = f"{self.base_url}/v1/completion"
        payload = {
            "prompt_name": prompt_name,
            "variables": variables,
            "model_override": model_override
        }

        last_error: Optional[Exception] = None
        for attempt in range(_MAX_RETRIES + 1):
            try:
                response = requests.post(
                    url, json=payload, timeout=(_CONNECT_TIMEOUT, _READ_TIMEOUT)
                )
                if response.status_code in _RETRIABLE_STATUSES and attempt < _MAX_RETRIES:
                    retry_after = response.headers.get("Retry-After")
                    delay = float(retry_after) if (retry_after and retry_after.replace(".", "", 1).isdigit()) else _retry_delay(attempt)
                    logger.warning(
                        "LLM Service returned %s, retrying in %.2fs (attempt %d/%d)",
                        response.status_code, delay, attempt + 1, _MAX_RETRIES,
                    )
                    time.sleep(delay)
                    continue
                response.raise_for_status()
                return response.json()
            except requests.exceptions.Timeout as e:
                last_error = e
                if attempt < _MAX_RETRIES:
                    delay = _retry_delay(attempt)
                    logger.warning(
                        "LLM Service timeout after %ds, retrying in %.2fs (attempt %d/%d)",
                        _READ_TIMEOUT, delay, attempt + 1, _MAX_RETRIES,
                    )
                    time.sleep(delay)
                    continue
                logger.error("LLM Service call timed out after %ds: %s", _READ_TIMEOUT, url)
                raise
            except requests.exceptions.ConnectionError as e:
                last_error = e
                if attempt < _MAX_RETRIES:
                    delay = _retry_delay(attempt)
                    logger.warning(
                        "LLM Service connection error, retrying in %.2fs (attempt %d/%d): %s",
                        delay, attempt + 1, _MAX_RETRIES, e,
                    )
                    time.sleep(delay)
                    continue
                logger.error("LLM Service connection error: %s", e)
                raise
            except requests.exceptions.RequestException as e:
                logger.error("LLM Service call failed: %s", e)
                raise

        # Should not reach here, but be explicit if all retries were exhausted on retriable status.
        if last_error is not None:
            raise last_error
        raise requests.exceptions.RequestException(f"LLM Service call failed after {_MAX_RETRIES + 1} attempts")

