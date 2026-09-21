"""Every prompt-registry call site must honour the deployment's own LLM_MODEL.

`PromptRegistry.get_config` (src/utils/prompt_registry.py) has no environment
awareness whatsoever — it returns the YAML's `model:` key, or the string
"gpt-3.5-turbo" when the YAML omits one. Sixteen prompt YAMLs hard-code
`model: gpt-4o`. That is harmless as a *default* and deliberately so: the call
site is what decides whether the operator's LLM_MODEL wins.

`/v1/diagnose/pipeline` was the one endpoint that did not. It passed
`config["model"]` straight to the provider, so a self-hosted stack configured
for Ollama asked Ollama for `gpt-4o`, which does not exist there:

    HTTP 502 {"detail":"LLM call failed: Error code: 404 -
     {'error': {'message': "model 'gpt-4o' not found", ...}}"}

Reproduced live on a GCP install running LLM_PROVIDER=ollama /
LLM_MODEL=qwen2.5:7b, where `ollama list` held exactly qwen2.5:7b. Every other
consumer resolved from the environment, which is why only Diagnose failed and
why it looked like a model-config problem rather than a one-line call-site bug.

Two guards, because the unit test alone would not have prevented this — the
defect was never that resolution was wrong, it was that ONE site skipped it:

1. `resolve_model` implements the order: override → LLM_MODEL → YAML default.
2. A census proves no call site bypasses it by reading `config["model"]`
   directly. This is the guard that generalises: a new endpoint that copies the
   old pattern fails here rather than in a customer's Diagnose panel.

The census first matched only a subscript on a variable named `config`, so SQL
generation's `cfg.get("model")` walked past it: on an OpenAI-protocol endpoint
(Groq, Vertex AI, OpenRouter) the Data Explorer asked for the YAML's gpt-4o
while every other prompt used LLM_MODEL. It now matches the key however it is
read and whatever the dict is called.

This is the same defect class as the divergent Explorer provider resolution
pinned in test_explorer_offline_resolution.py — a second copy of "which model
do we call" that drifted from the one the environment configures.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[1] / "src"
GATEWAY_MAIN = SRC / "gateway" / "main.py"


# ---------------------------------------------------------------------------
# 1. The resolution order itself.


def _resolve_model():
    """Import lazily: the gateway module builds clients at import time."""
    from src.gateway import main

    return main


def test_env_model_beats_the_prompt_yaml_default(monkeypatch):
    main = _resolve_model()
    monkeypatch.setattr(main, "DEFAULT_LLM_MODEL", "qwen2.5:7b")
    # What the diagnose prompt YAML actually carries.
    config = {"model": "gpt-4o"}
    assert main.resolve_model(config) == "qwen2.5:7b", (
        "the YAML default won over LLM_MODEL — this is the exact 404 that took "
        "Diagnose down on every install whose model is not literally gpt-4o"
    )


def test_per_request_override_beats_the_env_model(monkeypatch):
    main = _resolve_model()
    monkeypatch.setattr(main, "DEFAULT_LLM_MODEL", "qwen2.5:7b")
    assert main.resolve_model({"model": "gpt-4o"}, "llama-3.3-70b-versatile") == (
        "llama-3.3-70b-versatile"
    )


def test_yaml_default_is_used_when_no_env_model_is_set(monkeypatch):
    # The fallback must survive: an operator who sets no LLM_MODEL still gets a
    # working call, which is why this is a resolution ORDER and not a rewrite.
    main = _resolve_model()
    monkeypatch.setattr(main, "DEFAULT_LLM_MODEL", "")
    monkeypatch.setattr(main, "DEFAULT_LLM_PROVIDER", "openai")
    assert main.resolve_model({"model": "gpt-4o"}) == "gpt-4o"


@pytest.mark.parametrize(
    ("provider", "env", "expected"),
    [
        ("groq", {}, "llama-3.3-70b-versatile"),
        ("azure", {"AZURE_OPENAI_DEPLOYMENT": "prod-chat"}, "prod-chat"),
        ("ollama", {"OLLAMA_MODEL": "qwen2.5:7b"}, "qwen2.5:7b"),
    ],
)
def test_the_yaml_default_is_not_sent_to_a_provider_that_lacks_it(monkeypatch, provider, env, expected):
    # The YAML's gpt-4o is an OpenAI catalog name. A Groq, Azure or Ollama stack
    # with no LLM_MODEL asked for it and got a 404 on every prompt.
    main = _resolve_model()
    monkeypatch.delenv("LLM_MODEL", raising=False)
    for key in ("AZURE_OPENAI_DEPLOYMENT", "OLLAMA_MODEL"):
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(main, "DEFAULT_LLM_MODEL", "")
    monkeypatch.setattr(main, "DEFAULT_LLM_PROVIDER", provider)
    assert main.resolve_model({"model": "gpt-4o"}) == expected
    # A caller whose client was built for another provider says so.
    monkeypatch.setattr(main, "DEFAULT_LLM_PROVIDER", "openai")
    assert main.resolve_model({"model": "gpt-4o"}, None, provider) == expected


@pytest.mark.parametrize("empty", ["", None])
def test_an_empty_override_does_not_blank_the_model(monkeypatch, empty):
    # `model_override` is `str | None` and arrives unset on almost every request;
    # neither spelling may shadow the env model.
    main = _resolve_model()
    monkeypatch.setattr(main, "DEFAULT_LLM_MODEL", "qwen2.5:7b")
    assert main.resolve_model({"model": "gpt-4o"}, empty) == "qwen2.5:7b"


# ---------------------------------------------------------------------------
# 2. The census: no call site may bypass resolve_model.


def _config_model_reads(tree: ast.AST) -> list[int]:
    """Line numbers of every read of a "model" key in a module.

    `x["model"]` and `x.get("model", ...)`, whatever `x` is named: the prompt
    config has been bound as `config`, `cfg` and `repair_cfg`.
    """
    hits = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Subscript):
            key = node.slice
            if isinstance(key, ast.Constant) and key.value == "model":
                hits.append(node.lineno)
        elif isinstance(node, ast.Call):
            func = node.func
            if not (isinstance(func, ast.Attribute) and func.attr == "get" and node.args):
                continue
            key = node.args[0]
            if isinstance(key, ast.Constant) and key.value == "model":
                hits.append(node.lineno)
    return hits


def test_the_census_sees_both_ways_of_reading_the_key():
    """Arms the census with a control: both spellings that have shipped must hit."""
    tree = ast.parse(
        "def f(config, cfg):\n"
        "    a = config['model']\n"
        "    b = cfg.get('model') or 'x'\n"
        "    c = cfg.get('parameters')\n"
    )
    assert _config_model_reads(tree) == [2, 3]


def _resolve_model_body_lines(tree: ast.AST) -> set[int]:
    """The line span of resolve_model — the ONE place allowed to read the key."""
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "resolve_model":
            return set(range(node.lineno, (node.end_lineno or node.lineno) + 1))
    return set()


def test_resolve_model_exists_and_is_the_only_reader_of_the_yaml_key():
    tree = ast.parse(GATEWAY_MAIN.read_text())

    allowed = _resolve_model_body_lines(tree)
    assert allowed, (
        "resolve_model is gone from gateway/main.py — without it every call site "
        "is back to reading the prompt YAML directly"
    )

    stray = [ln for ln in _config_model_reads(tree) if ln not in allowed]
    assert not stray, (
        f"{GATEWAY_MAIN.name} reads the prompt config's \"model\" key directly at line(s) {stray}; "
        "that bypasses LLM_MODEL and sends the YAML's literal gpt-4o to whatever "
        "provider is configured. Call resolve_model(config) instead."
    )


def test_every_chat_completion_call_resolves_its_model():
    """The positive denominator.

    An absence assertion over call sites is worthless if the walk finds none —
    a renamed client or a refactor to a helper would make the test above pass
    vacuously. Count the call sites first and require that they exist.
    """
    tree = ast.parse(GATEWAY_MAIN.read_text())

    create_calls = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not (isinstance(node.func, ast.Attribute) and node.func.attr == "create"):
            continue
        for kw in node.keywords:
            if kw.arg == "model":
                create_calls.append((node.lineno, kw.value))

    assert len(create_calls) >= 2, (
        f"found only {len(create_calls)} completion call site(s) passing model= in "
        f"{GATEWAY_MAIN.name}; the walk stopped matching, so the guard below would "
        "pass without checking anything"
    )

    offenders = []
    for lineno, value in create_calls:
        # Accept resolve_model(...) directly, or a local bound from it.
        if isinstance(value, ast.Call) and isinstance(value.func, ast.Name):
            if value.func.id == "resolve_model":
                continue
        if isinstance(value, ast.Name):
            continue  # a local; the stray-subscript census above covers its source
        offenders.append(lineno)

    assert not offenders, (
        f"completion call(s) at line(s) {offenders} build their model inline "
        "instead of calling resolve_model — route them through it so LLM_MODEL "
        "cannot be skipped again"
    )


def test_the_prompt_yamls_still_carry_a_default_worth_overriding():
    """Anchors the premise, so this suite cannot quietly become about nothing.

    If the YAMLs stopped hard-coding a hosted model the guards above would still
    pass while protecting against a defect that no longer exists — and the next
    person would delete them as dead weight. They are NOT dead: the YAML default
    is the value that ships, and resolve_model is what keeps it from being sent.
    """
    prompts = Path(__file__).resolve().parents[1] / "prompts"
    hardcoded = [
        p
        for p in prompts.rglob("*.yaml")
        if "model: gpt-" in p.read_text()
    ]
    assert hardcoded, (
        "no prompt YAML hard-codes a hosted model any more — if that is "
        "deliberate, this suite's premise changed and it needs rewriting, not "
        "deleting: the resolution order is still the contract"
    )
    diagnose = prompts / "diagnose" / "pipeline_failure.yaml"
    assert diagnose in hardcoded, (
        "diagnose/pipeline_failure.yaml no longer carries a hard-coded model; "
        "that was the exact file whose gpt-4o reached Ollama as a 404"
    )
