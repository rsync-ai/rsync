"""
An install without an LLM is supported.

The pipeline intent parser is deterministic, raw SQL runs in the Data Explorer,
and existing connectors need no model, so nothing may fail or wait on an LLM
the operator never set up. The features that genuinely call a model answer
503 {"error": "llm_not_configured"} before touching one, so the UI can say
"set up an LLM first" instead of timing out against an Ollama nobody started.

Every route test below arms a model client that records its calls: a gate that
stopped firing would reach the client, and the assertion on the recorder fails
even where the route would otherwise degrade quietly.
"""

import sys
from pathlib import Path

import pytest
import requests
from fastapi import FastAPI
from fastapi.testclient import TestClient

repo_root = str(Path(__file__).parent.parent)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from src.gateway.main import app  # noqa: E402
from src.utils import llm_client as llm_client_mod  # noqa: E402
from src.utils.llm_gate import (  # noqa: E402
    LLM_NOT_CONFIGURED,
    register_llm_gate,
    require_llm,
    sql_llm_configured,
)
from src.utils.openai_client import explorer_llm_configured, llm_configured  # noqa: E402

_LLM_ENV = (
    "LLM_PROVIDER",
    "OPENAI_API_KEY",
    "AZURE_OPENAI_API_KEY",
    "AZURE_OPENAI_ENDPOINT",
    "GROQ_API_KEY",
    "EXPLORER_LLM_PROVIDER",
    "EXPLORER_SQL_PROVIDER",
    "EXPLORER_OFFLINE_ONLY",
    "EXPLORER_SQL_ALLOW_ONLINE",
    "RANK_TABLES_LLM_PROVIDER",
    "USE_MOCK_LLM",
)


@pytest.fixture(autouse=True)
def no_llm(monkeypatch):
    """The environment of an install that skipped the LLM step.

    OLLAMA_URL stays set on purpose: compose always sends it, so its presence
    must not count as an LLM being set up.
    """
    for var in _LLM_ENV:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("OLLAMA_URL", "http://ollama:11434")


class _RecordingClient:
    """An AsyncOpenAI stand-in that records every model call."""

    def __init__(self):
        self.calls = 0
        outer = self

        class _Completions:
            async def create(self, **_kwargs):
                outer.calls += 1
                raise AssertionError("the model was called without an LLM set up")

        self.chat = type("_Chat", (), {"completions": _Completions()})()
        self.completions = _Completions()


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def model(monkeypatch):
    """Arm every module-level model client the gated routes could reach."""
    from src.agents.explorer import api as explorer_api
    from src.agents.explorer import rank_tables
    from src.gateway import main as gateway_main

    recorder = _RecordingClient()
    monkeypatch.setattr(explorer_api, "explorer_client", recorder)
    monkeypatch.setattr(rank_tables, "_client", recorder)
    monkeypatch.setattr(gateway_main, "sql_client", recorder)
    monkeypatch.setattr(gateway_main, "default_client", recorder)
    return recorder


def _assert_llm_not_configured(response):
    assert response.status_code == 503, response.text
    body = response.json()
    assert body["error"] == LLM_NOT_CONFIGURED
    assert body["message"].startswith("Set up an LLM first")


_TABLES = [
    {
        "name": "users",
        "schema": "public",
        "row_count": 10,
        "columns": [{"name": "id", "type": "integer", "is_primary_key": True}],
    },
]


# ---------------------------------------------------------------------------
# What "set up" means
# ---------------------------------------------------------------------------

class TestLLMConfigured:
    def test_nothing_set_is_not_configured(self):
        assert llm_configured() is False

    def test_fallback_ollama_is_not_configured(self, monkeypatch):
        monkeypatch.setenv("LLM_PROVIDER", "openai")  # compose's default, no key
        assert llm_configured() is False

    def test_explicit_ollama_is_configured(self, monkeypatch):
        monkeypatch.setenv("LLM_PROVIDER", "ollama")
        assert llm_configured() is True

    def test_cloud_key_is_configured(self, monkeypatch):
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
        assert llm_configured() is True

    @pytest.mark.parametrize("choice", ["none", "NONE", "disabled", "off", "false", "0"])
    def test_explicit_none_wins_over_a_leftover_key(self, monkeypatch, choice):
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
        monkeypatch.setenv("LLM_PROVIDER", choice)
        assert llm_configured() is False

    def test_explicit_argument_overrides_env(self, monkeypatch):
        monkeypatch.setenv("LLM_PROVIDER", "ollama")
        assert llm_configured("none") is False

    def test_offline_explorer_is_configured(self, monkeypatch):
        monkeypatch.setenv("EXPLORER_OFFLINE_ONLY", "true")
        assert explorer_llm_configured() is True
        assert llm_configured() is False

    def test_explorer_provider_overrides_stack_provider(self, monkeypatch):
        monkeypatch.setenv("LLM_PROVIDER", "ollama")
        monkeypatch.setenv("EXPLORER_LLM_PROVIDER", "none")
        assert explorer_llm_configured() is False

    # /api/v1/sql/generate uses EXPLORER_SQL_PROVIDER when it is set
    # (gateway/main.py sql_provider_resolved), so the gate must follow it both ways.
    def test_sql_provider_can_turn_sql_off(self, monkeypatch):
        monkeypatch.setenv("LLM_PROVIDER", "ollama")
        monkeypatch.setenv("EXPLORER_SQL_PROVIDER", "none")
        assert explorer_llm_configured() is True
        assert sql_llm_configured() is False

    def test_sql_provider_can_turn_sql_on(self, monkeypatch):
        monkeypatch.setenv("EXPLORER_SQL_PROVIDER", "ollama")
        assert explorer_llm_configured() is False
        assert sql_llm_configured() is True

    def test_sql_follows_the_explorer_when_unset(self, monkeypatch):
        assert sql_llm_configured() is False
        monkeypatch.setenv("EXPLORER_LLM_PROVIDER", "ollama")
        assert sql_llm_configured() is True

    def test_offline_only_pins_sql_to_ollama(self, monkeypatch):
        monkeypatch.setenv("EXPLORER_OFFLINE_ONLY", "true")
        monkeypatch.setenv("EXPLORER_SQL_PROVIDER", "none")
        assert sql_llm_configured() is True
        monkeypatch.setenv("EXPLORER_SQL_ALLOW_ONLINE", "true")
        assert sql_llm_configured() is False


# ---------------------------------------------------------------------------
# What keeps working
# ---------------------------------------------------------------------------

class TestWorksWithoutLLM:
    def test_health_is_ok_and_says_so(self, client):
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"
        assert response.json()["llm_configured"] is False

    def test_health_reports_a_configured_llm(self, client, monkeypatch):
        monkeypatch.setenv("LLM_PROVIDER", "ollama")
        assert client.get("/health").json()["llm_configured"] is True

    def test_chat_local_knowledge_still_answers(self, client, model):
        response = client.post(
            "/v1/completion",
            json={"prompt_name": "chat/assistant", "variables": {"user_message": "hello"}},
        )
        assert response.status_code == 200, response.text
        assert response.json()["model"] == "local-knowledge"
        assert model.calls == 0

    def test_next_steps_fall_back_to_rules(self, client, model):
        response = client.post(
            "/api/v1/explorer/nl/next-steps",
            json={
                "question": "",
                "sql": "SELECT count(*) FROM orders",
                "result_profile": {"row_count": 1, "columns": ["count"]},
            },
        )
        assert response.status_code == 200, response.text
        assert response.json()["suggestions"]
        assert model.calls == 0

    def test_rank_tables_falls_back_to_heuristic(self, client, model):
        response = client.post(
            "/agents/rank-tables",
            json={"tables": [{"name": "users"}, {"name": "orders"}], "intent": "sync users"},
        )
        assert response.status_code == 200, response.text
        assert [t["confidence"] for t in response.json()] == [0.3, 0.3]
        assert model.calls == 0

    def test_hitl_table_names_parse_without_a_model(self, client, monkeypatch):
        calls = []
        monkeypatch.setattr(
            llm_client_mod.LLMClient, "complete", lambda *a, **k: calls.append(a) or {}
        )
        response = client.post(
            "/v1/hitl/interpret",
            json={
                "node_id": "source_1",
                "node_kind": "source",
                "hitl_prompt": "Which tables do you want to sync?",
                "user_response": "users, orders",
            },
        )
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["success"] is True
        assert body["config_patch"] == {"tables": ["users", "orders"]}
        assert calls == []

    def test_hitl_free_text_asks_for_exact_names(self, client, monkeypatch):
        calls = []
        monkeypatch.setattr(
            llm_client_mod.LLMClient, "complete", lambda *a, **k: calls.append(a) or {}
        )
        response = client.post(
            "/v1/hitl/interpret",
            json={
                "node_id": "source_1",
                "node_kind": "source",
                "hitl_prompt": "Anything else before we start?",
                "user_response": "hmm, the usual ones I guess",
            },
        )
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["success"] is False
        assert body["error"] == LLM_NOT_CONFIGURED
        assert body["needs_clarification"] is True
        # Without the gate the route would call /v1/completion on itself.
        assert calls == []


# ---------------------------------------------------------------------------
# What says "set up an LLM first"
# ---------------------------------------------------------------------------

class TestGatedRoutes:
    def test_chat_beyond_local_knowledge(self, client, model):
        response = client.post(
            "/v1/completion",
            json={
                "prompt_name": "chat/assistant",
                "variables": {"user_message": "why is my pipeline lagging behind the source"},
            },
        )
        _assert_llm_not_configured(response)
        assert model.calls == 0

    def test_any_other_prompt(self, client, model):
        response = client.post(
            "/v1/completion",
            json={"prompt_name": "dag_planner/hitl_interpret", "variables": {}},
        )
        _assert_llm_not_configured(response)
        assert model.calls == 0

    def test_diagnose(self, client, model):
        response = client.post(
            "/v1/diagnose/pipeline",
            json={"pipeline_id": "p1", "evidence": {"status": "failed"}},
        )
        _assert_llm_not_configured(response)
        assert model.calls == 0

    def test_diagnose_still_validates_first(self, client, model):
        response = client.post("/v1/diagnose/pipeline", json={"pipeline_id": "", "evidence": {}})
        assert response.status_code == 400

    def test_sql_generate(self, client, model):
        response = client.post(
            "/api/v1/sql/generate",
            json={"question": "Count all users", "dialect": "postgresql", "schema": "users(id)"},
        )
        _assert_llm_not_configured(response)
        assert model.calls == 0

    def test_sql_generate_follows_its_own_provider(self, client, model, monkeypatch):
        # The Explorer has a model; SQL generation was switched off on its own.
        monkeypatch.setenv("EXPLORER_LLM_PROVIDER", "ollama")
        monkeypatch.setenv("EXPLORER_SQL_PROVIDER", "none")
        response = client.post(
            "/api/v1/sql/generate",
            json={"question": "Count all users", "dialect": "postgresql", "schema": "users(id)"},
        )
        _assert_llm_not_configured(response)
        assert model.calls == 0

    def test_resolve_tables(self, client, model):
        response = client.post(
            "/api/v1/explorer/nl/resolve-tables",
            json={"question": "Show me all users", "tables": _TABLES},
        )
        _assert_llm_not_configured(response)
        assert model.calls == 0

    def test_resolve_columns(self, client, model):
        response = client.post(
            "/api/v1/explorer/nl/resolve-columns",
            json={"question": "Show me user ids", "selected_tables": _TABLES},
        )
        _assert_llm_not_configured(response)
        assert model.calls == 0

    def test_mock_mode_is_not_gated(self, monkeypatch):
        # The routes' own USE_MOCK is fixed at import, so check the gates directly.
        from src.utils.llm_gate import require_explorer_llm, require_sql_llm

        monkeypatch.setenv("USE_MOCK_LLM", "true")
        require_llm()
        require_explorer_llm()
        require_sql_llm()


class TestResponseShape:
    """The gateway reads the flag from either shape; both must carry it."""

    def _app(self, register):
        mini = FastAPI()

        @mini.get("/x")
        async def _x():
            require_llm()
            return {"ok": True}

        if register:
            register_llm_gate(mini)
        return TestClient(mini)

    def test_registered_handler_serves_a_flat_body(self):
        _assert_llm_not_configured(self._app(register=True).get("/x"))

    def test_without_the_handler_it_nests_under_detail(self):
        response = self._app(register=False).get("/x")
        assert response.status_code == 503
        assert response.json()["detail"]["error"] == LLM_NOT_CONFIGURED


# ---------------------------------------------------------------------------
# Callers do not retry an answer that will not change
# ---------------------------------------------------------------------------

def _response(status, body=b""):
    r = requests.Response()
    r.status_code = status
    r._content = body
    r.headers["Content-Type"] = "application/json"
    r.url = "http://llm-service/v1/completion"
    return r


@pytest.mark.parametrize(
    "body",
    [
        b'{"error": "llm_not_configured", "message": "Set up an LLM first"}',
        b'{"detail": {"error": "llm_not_configured", "message": "Set up an LLM first"}}',
    ],
    ids=["flat", "nested"],
)
def test_llm_client_does_not_retry_llm_not_configured(monkeypatch, body):
    posts = []
    monkeypatch.setattr(
        llm_client_mod.requests, "post", lambda *a, **k: posts.append(1) or _response(503, body)
    )
    monkeypatch.setattr(llm_client_mod.time, "sleep", lambda _s: None)
    with pytest.raises(requests.exceptions.HTTPError):
        llm_client_mod.LLMClient().complete("chat/assistant", {})
    assert len(posts) == 1


def test_llm_client_still_retries_a_plain_503(monkeypatch):
    """Control: an ordinary 503 is transient and keeps its retries."""
    posts = []
    monkeypatch.setattr(
        llm_client_mod.requests, "post",
        lambda *a, **k: posts.append(1) or _response(503, b'{"detail": "busy"}'),
    )
    monkeypatch.setattr(llm_client_mod.time, "sleep", lambda _s: None)
    with pytest.raises(requests.exceptions.HTTPError):
        llm_client_mod.LLMClient().complete("chat/assistant", {})
    assert len(posts) == llm_client_mod._MAX_RETRIES + 1 > 1


# ---------------------------------------------------------------------------
# Deterministic HITL reading
# ---------------------------------------------------------------------------

class _NoModel:
    def __init__(self):
        self.calls = 0

    def complete(self, *_a, **_k):
        self.calls += 1
        raise AssertionError("the model was called without an LLM set up")


def _interpret(llm, prompt, reply, allow_llm):
    from src.agents.dag_planner.hitl_interpreter import interpret_node_input

    return interpret_node_input(
        llm_client=llm,
        node_id="n1",
        node_kind="source",
        hitl_prompt=prompt,
        user_response=reply,
        allow_llm=allow_llm,
    )


def test_hitl_keeps_a_lower_confidence_heuristic_without_a_model():
    llm = _NoModel()
    result = _interpret(llm, "Add a filter?", "where status = 'active'", allow_llm=False)
    assert result["success"] is True
    assert result["config_patch"] == {"filter": "where status = 'active'"}
    assert llm.calls == 0


def test_hitl_asks_for_exact_names_when_nothing_matches():
    llm = _NoModel()
    result = _interpret(llm, "Anything else?", "hmm, the usual ones I guess", allow_llm=False)
    assert result["success"] is False
    assert result["error"] == LLM_NOT_CONFIGURED
    assert result["needs_clarification"] is True
    assert "set up an LLM first" in result["clarification_prompt"]
    assert llm.calls == 0


def test_hitl_uses_the_model_when_one_is_set_up():
    """Control: the same unmatched reply does reach the model when allowed."""
    llm = _NoModel()
    _interpret(llm, "Anything else?", "hmm, the usual ones I guess", allow_llm=True)
    assert llm.calls == 1


# ---------------------------------------------------------------------------
# Transform suggestions
# ---------------------------------------------------------------------------

def test_suggestions_say_set_up_an_llm_for_transforms(client, monkeypatch):
    from src.agents.suggestions import service

    def _no_model(*_a, **_k):
        raise AssertionError("a model client was built without an LLM set up")

    monkeypatch.setattr(service, "make_sync_client", _no_model)
    response = client.post(
        "/api/v1/agents/suggestions/generate",
        json={
            "schema": {"columns": [{"name": "email", "type": "varchar"}]},
            "intent": {"description": "mask emails and only keep active users"},
        },
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["error_code"] == LLM_NOT_CONFIGURED
    assert body["error"].startswith("Set up an LLM first")
