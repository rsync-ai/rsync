"""
Google OAuth access tokens from the GCE metadata server, for Vertex AI.

Vertex AI's OpenAI-compatible endpoint accepts only a Google OAuth access token
as its bearer, and those tokens live for one hour. Pasting one into
OPENAI_API_KEY gave a stack that answered for sixty minutes and then failed
every LLM call with 401 until someone pasted a new token and restarted it.

With ``OPENAI_API_KEY_SOURCE=gcp-metadata`` the OpenAI client is handed
``MetadataAccessToken.get`` (or ``aget``) as its ``api_key``. The SDK calls a
callable key before every request, so a client built once at import keeps
sending a current token: this class caches the token and fetches a new one from
the VM's metadata server shortly before the old one expires.

``openai.auth.gcp_id_token_provider`` in the SDK is a different thing: it trades
a GCP *identity* token for an OpenAI token at auth.openai.com, which Vertex does
not accept.

The token is never logged and never put in an error message.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
import urllib.error
import urllib.request
from typing import Callable, Tuple

logger = logging.getLogger(__name__)

# The IP, not metadata.google.internal: a container on a user-defined Docker
# network resolves through Docker's embedded DNS, and the IP needs no DNS at all.
METADATA_TOKEN_URL = (
    "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token"
)
# Fetch a new token this long before the current one expires.
REFRESH_MARGIN_SECONDS = 300.0
FETCH_TIMEOUT_SECONDS = 5.0


class MetadataTokenError(RuntimeError):
    """The metadata server did not hand out a usable access token."""


def fetch_metadata_token(url: str = METADATA_TOKEN_URL) -> Tuple[str, float]:
    """One request to the metadata server: ``(access_token, expires_in_seconds)``."""
    request = urllib.request.Request(url, headers={"Metadata-Flavor": "Google"})
    # No proxy: HTTP(S)_PROXY in the container must not carry this request, or
    # the service account's token would be handed to the proxy.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request, timeout=FETCH_TIMEOUT_SECONDS) as response:
            body = response.read()
    except urllib.error.HTTPError as e:
        raise MetadataTokenError(
            f"the GCE metadata server answered HTTP {e.code} for the service account token. "
            "OPENAI_API_KEY_SOURCE=gcp-metadata needs a VM with a service account attached "
            "and the cloud-platform access scope."
        ) from None
    except (urllib.error.URLError, OSError) as e:
        raise MetadataTokenError(
            f"could not reach the GCE metadata server ({type(e).__name__}). "
            "OPENAI_API_KEY_SOURCE=gcp-metadata only works on a Google Compute Engine VM."
        ) from None
    try:
        data = json.loads(body)
        token = str(data["access_token"])
        expires_in = float(data["expires_in"])
    except (ValueError, KeyError, TypeError):
        raise MetadataTokenError(
            "the GCE metadata server returned a token response without access_token/expires_in"
        ) from None
    if not token:
        raise MetadataTokenError("the GCE metadata server returned an empty access token")
    return token, expires_in


class MetadataAccessToken:
    """A cached access token that refreshes itself before it expires.

    ``get`` is the sync client's ``api_key``; ``aget`` is the async client's. A
    refresh that fails while the cached token is still valid logs a warning and
    keeps using that token, so one slow metadata answer does not fail a request
    the token in hand could still serve.
    """

    def __init__(
        self,
        fetch: Callable[[], Tuple[str, float]] = fetch_metadata_token,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._fetch = fetch
        self._clock = clock
        self._lock = threading.Lock()
        self._token = ""
        self._expires_at = 0.0
        self._refresh_at = 0.0

    def _fresh(self) -> bool:
        return bool(self._token) and self._clock() < self._refresh_at

    def get(self) -> str:
        if self._fresh():
            return self._token
        with self._lock:
            if self._fresh():
                return self._token
            try:
                token, expires_in = self._fetch()
            except MetadataTokenError as e:
                if self._token and self._clock() < self._expires_at:
                    logger.warning("Vertex AI token refresh failed; using the current token until it expires: %s", e)
                    return self._token
                raise
            now = self._clock()
            # The metadata server hands out a cached token, so expires_in can be
            # below the margin. Re-asking on every request until it rotates would
            # put a metadata round trip in front of each call; halve instead.
            if expires_in > 2 * REFRESH_MARGIN_SECONDS:
                lifetime = expires_in - REFRESH_MARGIN_SECONDS
            else:
                lifetime = expires_in / 2
            self._token = token
            self._expires_at = now + expires_in
            self._refresh_at = now + lifetime
            return token

    async def aget(self) -> str:
        if self._fresh():
            return self._token
        return await asyncio.to_thread(self.get)
