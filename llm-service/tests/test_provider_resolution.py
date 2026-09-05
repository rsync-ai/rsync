"""H4 — LLM egress governance: Groq is opt-in only.

resolve_provider must never auto-select Groq from a stray GROQ_API_KEY (an
undisclosed external LLM egress). Groq is reachable only via an explicit
LLM_PROVIDER=groq. OpenAI/Azure auto-detect stays intact.
"""

import pytest

from src.utils.openai_client import resolve_provider

_ENV_KEYS = [
    "LLM_PROVIDER",
    "OPENAI_API_KEY",
    "AZURE_OPENAI_ENDPOINT",
    "AZURE_OPENAI_API_KEY",
    "GROQ_API_KEY",
]


@pytest.fixture(autouse=True)
def _clear_env(monkeypatch):
    for k in _ENV_KEYS:
        monkeypatch.delenv(k, raising=False)


def test_stray_groq_key_does_not_auto_select_groq(monkeypatch):
    # A GROQ_API_KEY present without an explicit LLM_PROVIDER must NOT route
    # prompts to Groq — that would be an undisclosed external LLM egress.
    monkeypatch.setenv("GROQ_API_KEY", "gsk_test")
    assert resolve_provider() == "ollama"


def test_groq_is_opt_in_via_explicit_provider(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "groq")
    monkeypatch.setenv("GROQ_API_KEY", "gsk_test")
    assert resolve_provider() == "groq"


def test_openai_autodetect_intact(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk_test")
    assert resolve_provider() == "openai"


def test_azure_takes_priority(monkeypatch):
    monkeypatch.setenv("AZURE_OPENAI_ENDPOINT", "https://x.openai.azure.com")
    monkeypatch.setenv("OPENAI_API_KEY", "sk_test")
    assert resolve_provider() == "azure"


def test_default_is_ollama():
    assert resolve_provider() == "ollama"
