"""A thinking model must have room left to answer after it thinks.

Prod runs gemini-3.6-flash through Google's OpenAI-compatible endpoint. Its hidden
reasoning comes out of the same max_tokens as the visible answer, and the prompt
YAMLs size max_tokens for the answer alone: 300 for intent classification, 250
for the help answer, 600 for Diagnose. The thinking used the cap up and the JSON
stopped after about ten tokens. Seen on app.rsync.ai on 2026-09-26:

  - llm_usage_events: all 5 chat/intent_classification calls on gemini-3.6-flash
    recorded 7-12 completion tokens against a 300 cap
  - api-gateway: "failed to parse intent content: unexpected end of JSON input"
    after a 200 from Google, so the chat said the model did not answer
  - Diagnose: '{\\n"summary": "The CDC pipeline is healthy and actively streaming
    data.",\\n"root', shown as "(model returned unstructured output)"

The fake below spends a fixed number of reasoning tokens out of max_tokens before
it writes a character, one token per character, and stops with
finish_reason="length" when the cap runs out, which is how the real one failed.
The controls use a model that does not think: it fits the YAML caps as they are,
so the failures here come from the thinking and not from the fake.
"""

from __future__ import annotations

import ast
import importlib
import json
import logging
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

repo_root = str(Path(__file__).parent.parent)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)



def _gateway():
    # Imported when a test runs, never at collection. test_explorer_endpoints.py
    # drops every src.* module while it is collected and imports the gateway again:
    # a gateway imported here first registers its Prometheus metrics twice
    # (DuplicateTimeseries fails that file's collection), and a module held from
    # before the purge is not the one the app then runs.
    return importlib.import_module("src.gateway.main")


def _openai_client():
    return importlib.import_module("src.utils.openai_client")


# Above every chat and Diagnose cap, under the default headroom.
THINKING_TOKENS = 1500

INTENT_ANSWER = json.dumps({
    "intent": "general_knowledge",
    "requires_execution": False,
    "parameters": {"topic": "why a pipeline stage runs slowly"},
})
HELP_ANSWER = json.dumps({
    "message": "A stage is slow when its source is busy.",
    "suggestions": ["Check the source load"],
})
SLOT_ANSWER = json.dumps({
    "answering_previous": True,
    "extracted_slot": "destination",
    "extracted_value": "bigquery",
    "confidence": 0.9,
})
DIAGNOSE_ANSWER = json.dumps({
    "summary": "The CDC pipeline is healthy and actively streaming data.",
    "root_cause": "No fault: the connector is running and the sink lag is zero.",
    "suggested_action": "No action needed.",
    "confidence": "high",
    "evidence_pointers": ["runtime.status"],
})


class _ThinkingModel:
    """An AsyncOpenAI stand-in whose reasoning is billed against max_tokens."""

    def __init__(self, answer: str, thinking_tokens: int):
        self.requests: list[dict] = []
        outer = self

        class _Completions:
            async def create(self, **kwargs):
                outer.requests.append(kwargs)
                room = max(0, int(kwargs["max_tokens"]) - thinking_tokens)
                visible = answer[:room]
                finish = "stop" if len(visible) == len(answer) else "length"
                usage = SimpleNamespace(
                    prompt_tokens=100,
                    # Google leaves the reasoning out of completion_tokens, which
                    # is why llm_usage_events showed 7-12.
                    completion_tokens=len(visible),
                    total_tokens=100 + len(visible),
                    completion_tokens_details=SimpleNamespace(
                        reasoning_tokens=min(thinking_tokens, int(kwargs["max_tokens"]))
                    ),
                )
                return SimpleNamespace(
                    choices=[SimpleNamespace(
                        message=SimpleNamespace(content=visible),
                        finish_reason=finish,
                    )],
                    usage=usage,
                )

        self.chat = SimpleNamespace(completions=_Completions())
        self.completions = _Completions()


@pytest.fixture
def model(monkeypatch):
    """Arms the gateway with a model and returns a function that swaps it."""
    gateway_main = _gateway()
    monkeypatch.setattr(gateway_main, "USE_MOCK", False)
    monkeypatch.setattr(gateway_main, "require_llm", lambda *a, **k: None)
    import src.utils.llm_cost as llm_cost

    monkeypatch.setattr(llm_cost, "record_usage", lambda **_kw: 0)
    monkeypatch.delenv("LLM_REASONING_TOKEN_HEADROOM", raising=False)

    def use(answer: str, thinking_tokens: int) -> _ThinkingModel:
        fake = _ThinkingModel(answer, thinking_tokens)
        monkeypatch.setattr(gateway_main, "default_client", fake)
        return fake

    return use


def _complete(prompt_name: str) -> dict:
    client = TestClient(_gateway().app)
    response = client.post(
        "/v1/completion",
        json={
            "prompt_name": prompt_name,
            "variables": {
                "user_message": "In my pipeline, these stages are taking unusually long. Why?",
                "state": "awaiting_destination",
                "pending_source": "postgresql",
                "pending_destination": "",
                "last_response": "Where should the data go?",
            },
        },
    )
    assert response.status_code == 200, response.text
    return response.json()


def _diagnose() -> dict:
    client = TestClient(_gateway().app)
    response = client.post(
        "/v1/diagnose/pipeline",
        json={"pipeline_id": "p1", "evidence": {"runtime": {"status": "running"}}},
    )
    assert response.status_code == 200, response.text
    return response.json()


CHAT_PROMPTS = [
    ("chat/intent_classification", INTENT_ANSWER),
    ("chat/help_response", HELP_ANSWER),
    ("chat/slot_filling", SLOT_ANSWER),
]


# ---------------------------------------------------------------------------
# 1. The regression: the whole answer arrives from a model that thinks first.


@pytest.mark.parametrize(("prompt_name", "answer"), CHAT_PROMPTS)
def test_a_chat_prompt_gets_the_whole_answer_from_a_thinking_model(model, prompt_name, answer):
    model(answer, THINKING_TOKENS)
    body = _complete(prompt_name)
    assert body["content"] == answer, (
        f"{prompt_name} came back cut off at {len(body['content'])} characters: "
        f"{body['content']!r}. The model spent its max_tokens thinking; this is the "
        "'unexpected end of JSON input' the chat showed on prod."
    )
    assert body["finish_reason"] == "stop"


def test_diagnose_parses_a_thinking_models_answer(model):
    model(DIAGNOSE_ANSWER, THINKING_TOKENS)
    body = _diagnose()
    assert body["summary"] == "The CDC pipeline is healthy and actively streaming data.", body
    assert body["confidence"] == "high"
    assert body["raw"] is None


# ---------------------------------------------------------------------------
# 2. A reply still cut off after the headroom says so.


def test_a_cut_off_completion_says_it_was_cut_off(model, caplog):
    fake = model(INTENT_ANSWER, 100_000)
    with caplog.at_level(logging.WARNING):
        body = _complete("chat/intent_classification")
    assert body["finish_reason"] == "length"
    warned = [r.getMessage() for r in caplog.records if "cut off at max_tokens" in r.getMessage()]
    assert len(warned) == 1, [r.getMessage() for r in caplog.records]
    assert "chat/intent_classification" in warned[0]
    assert f"max_tokens={fake.requests[0]['max_tokens']}" in warned[0]
    assert "LLM_REASONING_TOKEN_HEADROOM" in warned[0]


def test_a_cut_off_diagnosis_says_it_was_cut_off(model):
    model(DIAGNOSE_ANSWER, 100_000)
    body = _diagnose()
    assert body["summary"] != "(model returned unstructured output)", (
        "a reply cut off at max_tokens was reported as the model ignoring the JSON "
        "format, which sends the reader after the prompt instead of the token budget"
    )
    assert "cut off" in body["summary"]
    assert "LLM_REASONING_TOKEN_HEADROOM" in body["suggested_action"]
    assert body["confidence"] == "low"


def test_a_diagnosis_that_is_not_json_is_still_called_unstructured(model):
    # The cut-off message must not swallow the case it replaced: a model that
    # finished and simply ignored the format.
    model("The pipeline looks fine to me.", 0)
    body = _diagnose()
    assert body["summary"] == "(model returned unstructured output)"
    assert body["raw"] == "The pipeline looks fine to me."


# ---------------------------------------------------------------------------
# 3. Controls: the fake is honest, and the headroom is what fixes it.


@pytest.mark.parametrize(("prompt_name", "answer"), CHAT_PROMPTS)
def test_control_a_model_that_does_not_think_fits_the_yaml_cap(model, prompt_name, answer):
    fake = model(answer, 0)
    body = _complete(prompt_name)
    assert body["content"] == answer
    cap = _gateway().registry.get_config(prompt_name)["parameters"]["max_tokens"]
    assert len(answer) <= cap, "the fake's answer is longer than the YAML cap it is meant to fit"
    assert fake.requests[0]["max_tokens"] >= cap


def test_control_without_headroom_the_thinking_model_is_cut_off(model, monkeypatch):
    monkeypatch.setenv("LLM_REASONING_TOKEN_HEADROOM", "0")
    fake = model(INTENT_ANSWER, THINKING_TOKENS)
    body = _complete("chat/intent_classification")
    assert fake.requests[0]["max_tokens"] == 300, "0 must send the YAML cap unchanged"
    assert body["finish_reason"] == "length"
    assert body["content"] != INTENT_ANSWER


# ---------------------------------------------------------------------------
# 4. The headroom itself.


def test_the_headroom_is_added_to_the_answer_budget(monkeypatch):
    monkeypatch.delenv("LLM_REASONING_TOKEN_HEADROOM", raising=False)
    openai_client = _openai_client()
    assert openai_client.with_reasoning_headroom(300) == 300 + openai_client.DEFAULT_REASONING_TOKEN_HEADROOM
    assert openai_client.DEFAULT_REASONING_TOKEN_HEADROOM > THINKING_TOKENS


@pytest.mark.parametrize(("raw", "expected_extra"), [("0", 0), ("8192", 8192), (" 2048 ", 2048)])
def test_the_headroom_is_configurable(monkeypatch, raw, expected_extra):
    monkeypatch.setenv("LLM_REASONING_TOKEN_HEADROOM", raw)
    assert _openai_client().with_reasoning_headroom(300) == 300 + expected_extra


@pytest.mark.parametrize("raw", ["lots", "-1", "4k"])
def test_a_bad_headroom_falls_back_to_the_default_and_says_so(monkeypatch, caplog, raw):
    monkeypatch.setenv("LLM_REASONING_TOKEN_HEADROOM", raw)
    openai_client = _openai_client()
    monkeypatch.setattr(openai_client, "_FALLBACK_WARNED", set())
    with caplog.at_level(logging.WARNING):
        got = openai_client.with_reasoning_headroom(300)
    assert got == 300 + openai_client.DEFAULT_REASONING_TOKEN_HEADROOM
    assert "LLM_REASONING_TOKEN_HEADROOM" in caplog.text


# ---------------------------------------------------------------------------
# 5. The census: every prompt call passes its max_tokens through the headroom.

SRC = Path(__file__).resolve().parents[1] / "src"
HELPER = "with_reasoning_headroom"
# tool_generator sizes its own output budget (RSYNC_LLM_MAX_OUTPUT_TOKENS, 16k for
# doc extraction) and already raises on finish_reason="length".
EXEMPT = ("agents/tool_generator/",)


def _is_helper_call(node: ast.AST) -> bool:
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
    return name == HELPER


def _unbudgeted_create_calls(tree: ast.AST) -> tuple[int, list[int]]:
    """(max_tokens= create calls found, lines whose value skips the helper).

    A value passes when it is the helper call itself, or a name that every
    assignment in the enclosing function binds from the helper. One raw
    assignment, in any branch, is enough to fail the name.
    """
    found: set[int] = set()
    bad: set[int] = set()
    for func in ast.walk(tree):
        if not isinstance(func, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        from_helper: dict[str, bool] = {}
        for node in ast.walk(func):
            if isinstance(node, ast.Assign):
                targets, value = node.targets, node.value
            elif isinstance(node, (ast.AnnAssign, ast.AugAssign)):
                targets, value = [node.target], node.value
            else:
                continue
            for target in targets:
                if isinstance(target, ast.Name):
                    from_helper[target.id] = from_helper.get(target.id, True) and (
                        isinstance(node, ast.Assign | ast.AnnAssign) and _is_helper_call(value)
                    )
        budgeted = {name for name, ok in from_helper.items() if ok}
        for node in ast.walk(func):
            if not (isinstance(node, ast.Call) and getattr(node.func, "attr", None) == "create"):
                continue
            for kw in node.keywords:
                if kw.arg != "max_tokens":
                    continue
                found.add(node.lineno)
                value = kw.value
                if _is_helper_call(value) or (isinstance(value, ast.Name) and value.id in budgeted):
                    continue
                bad.add(node.lineno)
    return len(found), sorted(bad)


def test_the_census_flags_a_raw_cap_and_passes_a_budgeted_one():
    tree = ast.parse(
        "async def f(client, cfg):\n"
        "    a = await client.chat.completions.create(model='m', max_tokens=300)\n"
        "    b = await client.chat.completions.create(model='m', max_tokens=with_reasoning_headroom(300))\n"
        "    n = with_reasoning_headroom(cfg['parameters'].get('max_tokens', 512))\n"
        "    c = await client.completions.create(model='m', max_tokens=n)\n"
        "    raw = cfg['parameters'].get('max_tokens', 512)\n"
        "    d = await client.completions.create(model='m', max_tokens=raw)\n"
        "    if cfg:\n"
        "        m = with_reasoning_headroom(300)\n"
        "    else:\n"
        "        m = cfg['parameters'].get('max_tokens', 512)\n"
        "    e = await client.completions.create(model='m', max_tokens=m)\n"
        "    n += 1\n"
    )
    # e: wrapped in one branch, raw in the other. n: grown after it was wrapped
    # (c still reads it, and passes, because the census does not order statements;
    # the += is what fails it).
    assert _unbudgeted_create_calls(tree) == (5, [2, 5, 7, 12])


def test_every_prompt_call_leaves_room_to_think():
    total = 0
    per_file: dict[str, int] = {}
    offenders: list[str] = []
    for path in sorted(SRC.rglob("*.py")):
        rel = path.relative_to(SRC).as_posix()
        if rel.startswith(EXEMPT):
            continue
        count, bad = _unbudgeted_create_calls(ast.parse(path.read_text()))
        if count:
            per_file[rel] = count
        total += count
        offenders += [f"{rel}:{ln}" for ln in bad]

    # The denominator: a walk that stopped matching would pass on nothing.
    assert per_file.get("gateway/main.py", 0) >= 15, per_file
    assert per_file.get("agents/explorer/api.py", 0) >= 3, per_file
    assert total >= 20, per_file
    assert not offenders, (
        f"completion call(s) at {offenders} pass a max_tokens that has no room for a "
        f"thinking model's reasoning. Wrap the answer budget in {HELPER}(...): on "
        "gemini-3.6-flash a bare 300 left about ten tokens for the answer."
    )
