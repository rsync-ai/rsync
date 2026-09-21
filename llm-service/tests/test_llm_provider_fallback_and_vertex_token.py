"""The LLM setup must not fail silently, and a Vertex AI token must not expire.

Three ways a self-hosted stack looked configured and was not:

1. ``LLM_PROVIDER=groq`` (or azure, or openai) with the key missing resolved to
   the local Ollama fallback without a word in the log. The quickstart compose
   never passed GROQ_API_KEY or AZURE_OPENAI_* to the services, so that was the
   state of every Groq and Azure install. An unknown name (``LLM_PROVIDER=vertex``)
   went to auto-detect just as quietly. Each is now logged once.

2. Vertex AI's OpenAI-compatible endpoint takes a Google OAuth access token that
   lasts an hour. Pasted into OPENAI_API_KEY, the stack worked for sixty minutes
   and then failed every call. ``OPENAI_API_KEY_SOURCE=gcp-metadata`` hands the
   SDK a function it calls before each request; the function refreshes the token
   from the VM's metadata server before it expires.

3. That token is the VM service account's, good for any Google API the account
   can reach, so it is only ever sent to an https googleapis.com host.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer

import httpx2
import pytest

from _cut_collection import tree_is_intact
from src.utils import gcp_access_token, openai_client
from src.utils.gcp_access_token import MetadataAccessToken, MetadataTokenError, fetch_metadata_token
from src.utils.openai_client import (
    llm_configured,
    make_async_client,
    make_sync_client,
    openai_api_key,
    resolve_provider,
)

VERTEX_BASE = (
    "https://us-central1-aiplatform.googleapis.com/v1/projects/demo/locations/us-central1/endpoints/openapi"
)

_LLM_ENV = (
    "LLM_PROVIDER",
    "LLM_MODEL",
    "OPENAI_API_KEY",
    "OPENAI_API_KEY_SOURCE",
    "OPENAI_BASE_URL",
    "AZURE_OPENAI_API_KEY",
    "AZURE_OPENAI_ENDPOINT",
    "AZURE_OPENAI_DEPLOYMENT",
    "AZURE_OPENAI_API_VERSION",
    "GROQ_API_KEY",
    "EXPLORER_LLM_PROVIDER",
    "EXPLORER_OFFLINE_ONLY",
)


@pytest.fixture(autouse=True)
def clean_llm_env(monkeypatch):
    for var in _LLM_ENV:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("OLLAMA_URL", "http://ollama:11434")
    # Warnings are once per process; each test starts fresh, with no cached token.
    monkeypatch.setattr(openai_client, "_PLACEHOLDER_WARNED", set())
    monkeypatch.setattr(openai_client, "_FALLBACK_WARNED", set())
    monkeypatch.setattr(openai_client, "_GCP_TOKEN", None)


class _FakeMetadata:
    """A metadata server: hands out tok1, tok2, ... each with the given lifetime."""

    def __init__(self, expires_in: float = 3599.0):
        self.expires_in = expires_in
        self.calls = 0
        self.fail = False

    def __call__(self):
        if self.fail:
            raise MetadataTokenError("metadata server unreachable")
        self.calls += 1
        return f"tok{self.calls}", self.expires_in


class _Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


# ── 1. The token cache ────────────────────────────────────────────────────────


def test_the_token_is_fetched_once_and_reused_while_fresh():
    meta, clock = _FakeMetadata(), _Clock()
    token = MetadataAccessToken(fetch=meta, clock=clock)
    assert token.get() == "tok1"
    clock.now += 60
    assert token.get() == "tok1"
    assert meta.calls == 1


def test_the_token_is_replaced_before_it_expires():
    meta, clock = _FakeMetadata(expires_in=3600.0), _Clock()
    token = MetadataAccessToken(fetch=meta, clock=clock)
    assert token.get() == "tok1"

    # Still inside the hour but within the refresh margin: a request sent now
    # must not carry a token that could expire in flight.
    clock.now += 3600 - gcp_access_token.REFRESH_MARGIN_SECONDS + 1
    assert token.get() == "tok2"
    assert meta.calls == 2

    # And the replacement is itself refreshed, so the second hour works too.
    clock.now += 3600
    assert token.get() == "tok3"


def test_a_short_lived_token_is_not_refetched_on_every_request():
    # The metadata server returns its cached token, so expires_in can be below
    # the margin. Refreshing "margin seconds early" would mean every request.
    meta, clock = _FakeMetadata(expires_in=120.0), _Clock()
    token = MetadataAccessToken(fetch=meta, clock=clock)
    token.get()
    clock.now += 30
    token.get()
    assert meta.calls == 1
    clock.now += 31
    assert token.get() == "tok2"


def test_a_failed_refresh_keeps_a_token_that_has_not_expired(caplog):
    meta, clock = _FakeMetadata(expires_in=3600.0), _Clock()
    token = MetadataAccessToken(fetch=meta, clock=clock)
    token.get()
    clock.now += 3400  # past the refresh point, before expiry
    meta.fail = True
    with caplog.at_level(logging.WARNING):
        assert token.get() == "tok1"
    assert "refresh failed" in caplog.text
    assert "tok1" not in caplog.text


def test_a_failed_refresh_of_an_expired_token_raises():
    meta, clock = _FakeMetadata(expires_in=3600.0), _Clock()
    token = MetadataAccessToken(fetch=meta, clock=clock)
    token.get()
    clock.now += 3601
    meta.fail = True
    with pytest.raises(MetadataTokenError):
        token.get()


def test_the_async_getter_refreshes_too():
    meta, clock = _FakeMetadata(expires_in=3600.0), _Clock()
    token = MetadataAccessToken(fetch=meta, clock=clock)
    assert asyncio.run(token.aget()) == "tok1"
    clock.now += 3500
    assert asyncio.run(token.aget()) == "tok2"


# ── 2. The metadata request itself ────────────────────────────────────────────


class _MetadataHandler(BaseHTTPRequestHandler):
    status = 200
    body = b""
    seen_flavor: list = []

    def do_GET(self):  # noqa: N802
        type(self).seen_flavor.append(self.headers.get("Metadata-Flavor"))
        self.send_response(self.status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(self.body)

    def log_message(self, *args):
        pass


@pytest.fixture
def metadata_server():
    handler = type("H", (_MetadataHandler,), {"seen_flavor": []})
    server = HTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield handler, f"http://127.0.0.1:{server.server_address[1]}/token"
    finally:
        server.shutdown()
        server.server_close()


def test_the_request_carries_the_metadata_flavor_header_and_parses(metadata_server):
    handler, url = metadata_server
    handler.body = json.dumps({"access_token": "ya29.test", "expires_in": 3599, "token_type": "Bearer"}).encode()
    assert fetch_metadata_token(url) == ("ya29.test", 3599.0)
    # The metadata server refuses a request without it.
    assert handler.seen_flavor == ["Google"]


def test_an_error_answer_raises_without_echoing_the_body(metadata_server):
    handler, url = metadata_server
    handler.status = 403
    handler.body = b'{"secret-ish": "ya29.should-not-appear"}'
    with pytest.raises(MetadataTokenError) as err:
        fetch_metadata_token(url)
    assert "403" in str(err.value)
    assert "ya29" not in str(err.value)


def test_the_request_bypasses_any_configured_proxy(monkeypatch):
    # HTTP_PROXY in the container must not carry the service account token.
    handlers = []
    real_build_opener = urllib.request.build_opener

    def spy(*hs):
        handlers.extend(hs)
        return real_build_opener(*hs)

    monkeypatch.setattr(urllib.request, "build_opener", spy)
    with pytest.raises(MetadataTokenError):
        fetch_metadata_token("http://127.0.0.1:9/token")
    proxy_handlers = [h for h in handlers if isinstance(h, urllib.request.ProxyHandler)]
    assert proxy_handlers and all(h.proxies == {} for h in proxy_handlers)


# ── 3. The key source is only used on a Google host ───────────────────────────


def _use_fake_token(monkeypatch, meta, clock):
    monkeypatch.setattr(openai_client, "_GCP_TOKEN", MetadataAccessToken(fetch=meta, clock=clock))


def test_vertex_with_the_metadata_source_counts_as_a_configured_llm(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("OPENAI_API_KEY_SOURCE", "gcp-metadata")
    monkeypatch.setenv("OPENAI_BASE_URL", VERTEX_BASE)
    assert resolve_provider() == "openai"
    assert llm_configured() is True


def test_the_metadata_source_is_picked_up_by_auto_detect(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY_SOURCE", "gcp-metadata")
    monkeypatch.setenv("OPENAI_BASE_URL", VERTEX_BASE)
    assert resolve_provider() == "openai"


@pytest.mark.parametrize(
    "base_url",
    [
        "",
        "https://api.openai.com/v1",
        "http://us-central1-aiplatform.googleapis.com/v1",  # not https
        "https://googleapis.com.attacker.example/v1",
        "https://attacker-googleapis.com/v1",
        "http://litellm:4000/v1",
    ],
)
def test_the_service_account_token_never_goes_to_another_host(monkeypatch, caplog, base_url):
    meta, clock = _FakeMetadata(), _Clock()
    _use_fake_token(monkeypatch, meta, clock)
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("OPENAI_API_KEY_SOURCE", "gcp-metadata")
    if base_url:
        monkeypatch.setenv("OPENAI_BASE_URL", base_url)

    with caplog.at_level(logging.WARNING):
        assert resolve_provider() == "ollama"
        assert llm_configured() is False
        assert openai_api_key() == ""
        assert openai_api_key(async_client=True) == ""
    assert meta.calls == 0
    assert "googleapis.com" in caplog.text


def test_an_unknown_key_source_falls_back_to_the_key_and_says_so(monkeypatch, caplog):
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("OPENAI_API_KEY_SOURCE", "sk-pasted-into-the-wrong-variable")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-" + "a1b2c3d4" * 6)
    with caplog.at_level(logging.WARNING):
        assert resolve_provider() == "openai"
        assert openai_api_key() == "sk-" + "a1b2c3d4" * 6
    assert "OPENAI_API_KEY_SOURCE" in caplog.text
    assert "pasted-into-the-wrong-variable" not in caplog.text


def test_without_a_key_source_the_static_key_is_sent_unchanged(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-" + "z9" * 20)
    assert openai_api_key() == "sk-" + "z9" * 20


# ── 4. The SDK sends a fresh token on every request ───────────────────────────


def _completion_response(request):
    return httpx2.Response(
        200,
        json={
            "id": "c",
            "object": "chat.completion",
            "created": 0,
            "model": "google/gemini-2.0-flash-001",
            "choices": [{"index": 0, "finish_reason": "stop", "message": {"role": "assistant", "content": "ok"}}],
        },
    )


def _vertex_env(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("OPENAI_API_KEY_SOURCE", "gcp-metadata")
    monkeypatch.setenv("OPENAI_BASE_URL", VERTEX_BASE)


def test_a_client_built_once_keeps_sending_a_current_token(monkeypatch):
    """The failure this replaces: a client built at import with an hour-long token."""
    meta, clock = _FakeMetadata(expires_in=3600.0), _Clock()
    _use_fake_token(monkeypatch, meta, clock)
    _vertex_env(monkeypatch)

    sent = []

    def handler(request):
        sent.append((request.url.host, request.headers.get("authorization")))
        return _completion_response(request)

    # Built through the factory, as every service builds it; only the transport is faked.
    client = make_sync_client().copy(http_client=httpx2.Client(transport=httpx2.MockTransport(handler)))
    ask = dict(model="google/gemini-2.0-flash-001", messages=[{"role": "user", "content": "hi"}])

    client.chat.completions.create(**ask)
    clock.now += 1800
    client.chat.completions.create(**ask)
    clock.now += 3600  # well past the first token's hour
    client.chat.completions.create(**ask)

    assert sent == [
        ("us-central1-aiplatform.googleapis.com", "Bearer tok1"),
        ("us-central1-aiplatform.googleapis.com", "Bearer tok1"),
        ("us-central1-aiplatform.googleapis.com", "Bearer tok2"),
    ]


def test_the_async_client_keeps_sending_a_current_token(monkeypatch):
    meta, clock = _FakeMetadata(expires_in=3600.0), _Clock()
    _use_fake_token(monkeypatch, meta, clock)
    _vertex_env(monkeypatch)

    sent = []

    def handler(request):
        sent.append(request.headers.get("authorization"))
        return _completion_response(request)

    client = make_async_client().copy(http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(handler)))
    ask = dict(model="google/gemini-2.0-flash-001", messages=[{"role": "user", "content": "hi"}])

    async def run():
        await client.chat.completions.create(**ask)
        clock.now += 3600
        await client.chat.completions.create(**ask)

    asyncio.run(run())
    assert sent == ["Bearer tok1", "Bearer tok2"]


def test_the_connector_generator_uses_the_same_token_source(monkeypatch):
    if not tree_is_intact():
        # The connector generator is the stripped half of tool_generator
        # (llm-service/oss-strip-list.txt); the public cut has nothing to import.
        pytest.skip("connector generator stripped by the public cut")
    from src.agents.tool_generator.agents import base

    meta, clock = _FakeMetadata(), _Clock()
    _use_fake_token(monkeypatch, meta, clock)
    _vertex_env(monkeypatch)
    assert base._detect_llm_provider() == "openai"
    key = openai_api_key()
    assert callable(key) and key() == "tok1"


# ── 5. A chosen provider without its credential is not silent ─────────────────


@pytest.mark.parametrize(
    ("provider", "names"),
    [
        ("groq", "GROQ_API_KEY"),
        ("azure", "AZURE_OPENAI_ENDPOINT"),
        ("openai", "OPENAI_API_KEY"),
    ],
)
def test_a_provider_missing_its_key_is_logged_once(monkeypatch, caplog, provider, names):
    monkeypatch.setenv("LLM_PROVIDER", provider)
    with caplog.at_level(logging.WARNING, logger="src.utils.openai_client"):
        assert resolve_provider() == "ollama"
        assert resolve_provider() == "ollama"
        assert llm_configured() is False
    records = [r for r in caplog.records if "Ollama fallback" in r.getMessage()]
    assert len(records) == 1, [r.getMessage() for r in caplog.records]
    assert names in records[0].getMessage()


def test_a_configured_provider_logs_no_fallback(monkeypatch, caplog):
    monkeypatch.setenv("LLM_PROVIDER", "groq")
    monkeypatch.setenv("GROQ_API_KEY", "gsk_" + "Q7" * 24)
    with caplog.at_level(logging.WARNING, logger="src.utils.openai_client"):
        assert resolve_provider() == "groq"
    assert "fallback" not in caplog.text


def test_an_unknown_provider_name_is_logged_once_without_its_value(monkeypatch, caplog):
    monkeypatch.setenv("LLM_PROVIDER", "made-up-provider-name")
    with caplog.at_level(logging.WARNING, logger="src.utils.openai_client"):
        assert resolve_provider() == "ollama"
        resolve_provider()
    records = [r for r in caplog.records if "does not know" in r.getMessage()]
    assert len(records) == 1
    assert "OPENAI_BASE_URL" in records[0].getMessage()
    assert "made-up-provider-name" not in caplog.text


@pytest.mark.parametrize("choice", ["", "ollama", "none", "off"])
def test_no_warning_for_the_choices_that_mean_what_they_say(monkeypatch, caplog, choice):
    if choice:
        monkeypatch.setenv("LLM_PROVIDER", choice)
    with caplog.at_level(logging.WARNING, logger="src.utils.openai_client"):
        resolve_provider()
    assert not caplog.records, [r.getMessage() for r in caplog.records]
