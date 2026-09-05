"""Explorer offline-mode resolution + the compose delivery path that carries it.

Two defect classes are pinned here:

1. **Divergent copies.** The gateway (`src/gateway/main.py`) and the explorer
   router (`src/agents/explorer/api.py`) each resolved the Explorer provider and
   its model defaults independently, and the copies disagreed — the router
   ignored LLM_MODEL, so on Azure it sent the literal "gpt-4o-mini" as a
   *deployment* name. Both now call `src.utils.openai_client`.

2. **No delivery path.** `docker-compose.quickstart.yml` read no env_file and did
   not list EXPLORER_OFFLINE_ONLY, so an operator who set it in `.env` got a
   privacy guarantee that silently did not hold. A flag the code reads is only
   real if the compose that ships it passes it through — that is asserted
   directly against the compose files, so the next such flag cannot regress.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

import pytest
import yaml

from src.utils.openai_client import (
    client_egress_host,
    env_bool,
    explorer_default_model,
    explorer_default_sql_model,
    make_async_client,
    rank_tables_default_model,
    resolve_explorer_provider,
    resolve_provider,
)

REPO_ROOT = Path(__file__).resolve().parents[2]

_ENV_KEYS = [
    "LLM_PROVIDER",
    "LLM_MODEL",
    "EXPLORER_LLM_PROVIDER",
    "EXPLORER_OFFLINE_ONLY",
    "OPENAI_API_KEY",
    "AZURE_OPENAI_ENDPOINT",
    "AZURE_OPENAI_API_KEY",
    "AZURE_OPENAI_DEPLOYMENT",
    "GROQ_API_KEY",
    "OLLAMA_MODEL",
    "RANK_TABLES_LLM_PROVIDER",
    "RANK_TABLES_MODEL",
    # Read by make_async_client when it builds the client, so they change where
    # a constructed client points. Cleared here or section 5 tests what the
    # developer's shell happens to say.
    "OPENAI_BASE_URL",
    "OLLAMA_BASE_URL",
    "OLLAMA_URL",
]


@pytest.fixture(autouse=True)
def _clear_env(monkeypatch):
    for k in _ENV_KEYS:
        monkeypatch.delenv(k, raising=False)


# ── 1. The offline flag actually pins the provider ────────────────────────────


def test_offline_only_overrides_a_configured_cloud_provider(monkeypatch):
    # The whole point of the flag: a working OpenAI config must still not be used.
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("OPENAI_API_KEY", "sk_test")
    monkeypatch.setenv("EXPLORER_OFFLINE_ONLY", "true")
    assert resolve_explorer_provider() == "ollama"


def test_offline_only_overrides_azure(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "azure")
    monkeypatch.setenv("AZURE_OPENAI_ENDPOINT", "https://x.openai.azure.com")
    monkeypatch.setenv("AZURE_OPENAI_API_KEY", "az_test")
    monkeypatch.setenv("EXPLORER_OFFLINE_ONLY", "true")
    assert resolve_explorer_provider() == "ollama"


def test_default_is_not_offline_and_inherits_the_stack(monkeypatch):
    # Default False — Explorer follows LLM_PROVIDER like everything else.
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("OPENAI_API_KEY", "sk_test")
    assert resolve_explorer_provider() == "openai"


def test_explicit_explorer_provider_wins_over_stack(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("OPENAI_API_KEY", "sk_test")
    monkeypatch.setenv("EXPLORER_LLM_PROVIDER", "ollama")
    assert resolve_explorer_provider() == "ollama"


def test_empty_passthrough_values_are_ignored(monkeypatch):
    # Compose passes these as soft-empty (`${VAR:-}`), so empty must mean "unset"
    # rather than pinning the Explorer to a provider named "".
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("OPENAI_API_KEY", "sk_test")
    monkeypatch.setenv("EXPLORER_LLM_PROVIDER", "")
    monkeypatch.setenv("EXPLORER_OFFLINE_ONLY", "")
    assert resolve_explorer_provider() == "openai"


# ── 1b. Empty must mean "unset", because that is what compose delivers ────────


def test_env_bool_treats_empty_as_unset_for_a_true_default(monkeypatch):
    """The landmine under `${VAR:-}` passthrough.

    Compose delivers an unset optional var as an empty string, not as an absent
    name. Reading that as False silently flips every flag whose default is True
    — so adding a harmless-looking passthrough line would change behaviour.
    """
    monkeypatch.setenv("EXPLORER_SQL_ALLOW_ONLINE", "")
    assert env_bool("EXPLORER_SQL_ALLOW_ONLINE", True) is True
    monkeypatch.setenv("EXPLORER_SQL_ALLOW_ONLINE", "   ")
    assert env_bool("EXPLORER_SQL_ALLOW_ONLINE", True) is True


def test_env_bool_still_honours_an_explicit_value(monkeypatch):
    monkeypatch.setenv("EXPLORER_OFFLINE_ONLY", "false")
    assert env_bool("EXPLORER_OFFLINE_ONLY", True) is False
    monkeypatch.setenv("EXPLORER_OFFLINE_ONLY", "TRUE")
    assert env_bool("EXPLORER_OFFLINE_ONLY", False) is True


_RESOLVING_MODULES = [
    "src/gateway/main.py",
    "src/agents/explorer/api.py",
    "src/agents/explorer/rank_tables.py",
]


@pytest.mark.parametrize("rel", _RESOLVING_MODULES)
def test_no_module_redefines_env_bool(rel):
    """Checked statically: importing src.gateway.main pulls in the whole service
    (sqlglot, kafka, otel), which is far too heavy for a unit test — but a local
    `def _env_bool` is exactly what re-introduces the empty-string trap, and that
    is visible in the source."""
    src = (REPO_ROOT / "llm-service" / rel).read_text()
    assert not re.search(r"^\s*def _env_bool\b", src, re.MULTILINE), (
        f"{rel} defines its own _env_bool — import env_bool from "
        "src.utils.openai_client instead, or the empty-value fix does not apply here"
    )


@pytest.mark.parametrize("rel", _RESOLVING_MODULES)
@pytest.mark.parametrize(
    "helper", ["_resolve_provider", "_create_async_client", "_ollama_base_url"]
)
def test_no_module_redefines_provider_resolution(rel, helper):
    """Same guard as above, for the three helpers that decide *where prompts go*.

    This is the one that already caught a live defect. main.py and api.py each
    carried a private `_resolve_provider` whose gate read
    `if p not in ("openai", "ollama", "groq")` — so LLM_PROVIDER=groq passed
    validation — and a private `_create_async_client` with branches for ollama
    and azure only. groq therefore fell through to the OpenAI branch and prompts
    went to api.openai.com. The copies also disagreed on timeout. A local
    definition of any of these three is how that returns.
    """
    src = (REPO_ROOT / "llm-service" / rel).read_text()
    assert not re.search(rf"^\s*def {helper}\b", src, re.MULTILINE), (
        f"{rel} defines its own {helper} — import it from src.utils.openai_client. "
        "Private copies of provider resolution are what sent groq traffic to OpenAI."
    )


@pytest.mark.parametrize("rel", _RESOLVING_MODULES)
def test_no_module_builds_an_sdk_client_directly(rel):
    """Constructing the SDK class in place is the same defect wearing a different
    name: the branch table lives at the call site again, and the next provider
    added to `resolve_provider` silently misroutes here. Every client comes from
    the shared factory."""
    src = (REPO_ROOT / "llm-service" / rel).read_text()
    hits = re.findall(r"\b(Async(?:Azure)?OpenAI)\s*\(", src)
    assert not hits, (
        f"{rel} constructs {sorted(set(hits))} directly — call "
        "make_async_client() so provider routing and the timeout stay in one place"
    )


# ── 2. Model defaults honour LLM_MODEL (the router/gateway divergence) ────────


def test_azure_default_model_uses_llm_model_not_the_literal_fallback(monkeypatch):
    # The regression: on Azure the model arg is a *deployment* name. Returning
    # "gpt-4o-mini" here is a 404 DeploymentNotFound unless a deployment happens
    # to carry that name. The explorer router used to do exactly that.
    monkeypatch.setenv("LLM_MODEL", "my-gpt4o-deployment")
    assert explorer_default_model("azure") == "my-gpt4o-deployment"


def test_azure_falls_back_to_deployment_name_when_llm_model_unset(monkeypatch):
    monkeypatch.setenv("AZURE_OPENAI_DEPLOYMENT", "prod-deployment")
    assert explorer_default_model("azure") == "prod-deployment"


def test_openai_default_model_uses_llm_model(monkeypatch):
    monkeypatch.setenv("LLM_MODEL", "gpt-4o")
    assert explorer_default_model("openai") == "gpt-4o"


def test_cloud_default_model_falls_back_when_nothing_configured():
    assert explorer_default_model("openai") == "gpt-4o-mini"


def test_ollama_default_model_is_a_local_model(monkeypatch):
    # LLM_MODEL is a cloud-side override; it must not leak a cloud model name
    # into the offline path, where it would name a model Ollama has not pulled.
    monkeypatch.setenv("LLM_MODEL", "gpt-4o")
    assert explorer_default_model("ollama") == "llama3:latest"


def test_ollama_sql_model_prefers_a_sql_specialist(monkeypatch):
    assert explorer_default_sql_model("ollama") == "sqlcoder:latest"
    monkeypatch.setenv("OLLAMA_MODEL", "qwen2.5:7b")
    assert explorer_default_sql_model("ollama") == "qwen2.5:7b"


def test_cloud_sql_model_reuses_the_chat_default(monkeypatch):
    monkeypatch.setenv("LLM_MODEL", "my-deployment")
    assert explorer_default_sql_model("azure") == "my-deployment"


def test_gateway_and_router_resolve_identically(monkeypatch):
    """The two live Explorer entry points must never disagree again.

    `resolve-tables` / `resolve-columns` / `next-steps` are served by the router;
    query-spec and text-to-SQL by the gateway. If these resolve differently, half
    the Explorer can be offline while the other half is not.
    """
    from src.agents.explorer import api as explorer_api
    from src.utils import openai_client

    assert explorer_api._resolve_explorer_provider is openai_client.resolve_explorer_provider
    assert explorer_api._explorer_default_model is openai_client.explorer_default_model


# ── 3. Every flag the code reads has a compose delivery path ──────────────────


class _ComposeLoader(yaml.SafeLoader):
    """SafeLoader that tolerates Compose's merge tags (`!override`, `!reset`)."""


_ComposeLoader.add_multi_constructor(
    "!", lambda loader, suffix, node: loader.construct_object(node.__class__(
        tag=f"tag:yaml.org,2002:{'seq' if isinstance(node, yaml.SequenceNode) else 'map' if isinstance(node, yaml.MappingNode) else 'str'}",
        value=node.value,
        start_mark=node.start_mark,
        end_mark=node.end_mark,
    ))
)


def _llm_service_env_keys(compose_path: Path) -> set[str]:
    """Env var names the llm-service container actually receives."""
    spec = yaml.load(compose_path.read_text(), Loader=_ComposeLoader)
    svc = spec["services"]["llm-service"]
    env = svc.get("environment") or {}
    if isinstance(env, dict):
        keys = set(env.keys())
    else:  # list form: "KEY=value"
        keys = {item.split("=", 1)[0].lstrip("-").strip() for item in env}
    # An env_file delivers the operator's whole .env, which covers everything.
    if svc.get("env_file"):
        keys.add("*")
    return keys


def _explorer_env_names_read_by_code() -> set[str]:
    """EXPLORER_*/RANK_TABLES_* names the service reads, scraped from source."""
    names: set[str] = set()
    for rel in _RESOLVING_MODULES:
        src = (REPO_ROOT / "llm-service" / rel).read_text()
        for pat in (
            r'os\.getenv\(\s*"((?:EXPLORER|RANK_TABLES)_[A-Z0-9_]+)"',
            r'_env_bool\(\s*"((?:EXPLORER|RANK_TABLES)_[A-Z0-9_]+)"',
            r'_resolve_explorer_provider\(\s*"([A-Z0-9_]+)"',
        ):
            names |= set(re.findall(pat, src))
    return names


@pytest.mark.parametrize(
    "compose_file",
    ["docker-compose.quickstart.yml", "docker-compose.prod.yml"],
)
def test_offline_flag_reaches_the_container(compose_file):
    """EXPLORER_OFFLINE_ONLY is a privacy control; a control with no delivery
    path is worse than none, because the operator believes it is on."""
    keys = _llm_service_env_keys(REPO_ROOT / compose_file)
    assert "*" in keys or "EXPLORER_OFFLINE_ONLY" in keys, (
        f"{compose_file} does not pass EXPLORER_OFFLINE_ONLY to llm-service. "
        "Setting it in .env would silently do nothing."
    )


@pytest.mark.parametrize(
    "compose_file",
    ["docker-compose.quickstart.yml", "docker-compose.prod.yml"],
)
def test_every_explorer_flag_the_code_reads_is_deliverable(compose_file):
    keys = _llm_service_env_keys(REPO_ROOT / compose_file)
    if "*" in keys:
        pytest.skip(f"{compose_file} uses env_file — all vars are deliverable")
    # Scraped from source rather than hardcoded, so a newly added EXPLORER_* knob
    # fails here until it is given a way in.
    missing = sorted(_explorer_env_names_read_by_code() - keys)
    assert not missing, (
        f"{compose_file} llm-service reads these but never receives them: {missing}"
    )


@pytest.mark.parametrize(
    "compose_file",
    ["docker-compose.quickstart.yml", "docker-compose.prod.yml"],
)
def test_ollama_base_url_is_deliverable(compose_file):
    """docs/deployment/env-vars.md documents OLLAMA_BASE_URL as the primary knob
    and it wins over OLLAMA_URL in code, so it needs its own passthrough."""
    keys = _llm_service_env_keys(REPO_ROOT / compose_file)
    assert "*" in keys or {"OLLAMA_BASE_URL", "OLLAMA_URL"} & keys, (
        f"{compose_file} gives llm-service no way to reach a non-default Ollama host"
    )


# ── 4. /agents/rank-tables — the third Explorer entry point ───────────────────
#
# This one resolved entirely on its own and had drifted furthest: it never read
# LLM_PROVIDER, never read EXPLORER_OFFLINE_ONLY, and defaulted to a cloud model
# name on every provider. It sends table names, column names, types and row
# counts, so all three were privacy-relevant, not merely untidy.


def test_rank_tables_honours_the_offline_flag(monkeypatch):
    monkeypatch.setenv("EXPLORER_OFFLINE_ONLY", "true")
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("OPENAI_API_KEY", "sk_test")
    assert resolve_explorer_provider("RANK_TABLES_LLM_PROVIDER") == "ollama"


def test_rank_tables_inherits_llm_provider_rather_than_sniffing_for_a_key(monkeypatch):
    """The exact production trap: an operator flips the stack to Ollama but
    leaves a now-unused OPENAI_API_KEY in .env. The old auto-detect saw the key,
    ignored LLM_PROVIDER, and kept shipping schema metadata to OpenAI."""
    monkeypatch.setenv("LLM_PROVIDER", "ollama")
    monkeypatch.setenv("OPENAI_API_KEY", "sk_stale_but_present")
    assert resolve_explorer_provider("RANK_TABLES_LLM_PROVIDER") == "ollama"


def test_rank_tables_override_still_wins_when_not_offline(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "ollama")
    monkeypatch.setenv("RANK_TABLES_LLM_PROVIDER", "openai")
    monkeypatch.setenv("OPENAI_API_KEY", "sk_test")
    assert resolve_explorer_provider("RANK_TABLES_LLM_PROVIDER") == "openai"


def test_rank_tables_offline_model_is_a_model_ollama_can_have(monkeypatch):
    # The old default returned the literal "gpt-4o-mini" for every non-Azure
    # provider, so even a correctly-configured offline deployment asked Ollama
    # for a model it has never pulled.
    assert rank_tables_default_model("ollama") == "llama3:latest"


def test_rank_tables_stays_cheap_on_openai(monkeypatch):
    # Ranking is a bulk metadata task pinned to a cheap model on purpose; it must
    # NOT follow a stack-wide LLM_MODEL upgrade and multiply its own cost.
    monkeypatch.setenv("LLM_MODEL", "gpt-4o")
    assert rank_tables_default_model("openai") == "gpt-4o-mini"


def test_rank_tables_azure_uses_a_deployment_name(monkeypatch):
    monkeypatch.setenv("AZURE_OPENAI_DEPLOYMENT", "prod-dep")
    assert rank_tables_default_model("azure") == "prod-dep"
    monkeypatch.delenv("AZURE_OPENAI_DEPLOYMENT")
    monkeypatch.setenv("LLM_MODEL", "fallback-dep")
    assert rank_tables_default_model("azure") == "fallback-dep"


def test_rank_tables_explicit_model_always_wins(monkeypatch):
    monkeypatch.setenv("RANK_TABLES_MODEL", "mixtral:8x7b")
    assert rank_tables_default_model("ollama") == "mixtral:8x7b"
    assert rank_tables_default_model("openai") == "mixtral:8x7b"


def test_rank_tables_refuses_a_per_request_provider_override_when_offline():
    """The request body carries an optional `provider`. Offline, that is a
    caller-controlled egress selector — assert the guard exists and precedes the
    client construction it protects."""
    src = (REPO_ROOT / "llm-service/src/agents/explorer/rank_tables.py").read_text()
    guard = src.index("if request.provider and EXPLORER_OFFLINE_ONLY:")
    build = src.index("client = _create_async_client(provider)")
    assert guard < build, "the offline guard must run before the override builds a client"



# ── 5. Where the constructed client actually points ───────────────────────────
#
# Sections 1–4 test the provider *string*. That string was never the bug: the
# defect was that `groq` resolved correctly and then built a client aimed at
# api.openai.com. Everything below reads the finished client object, because
# that is the only place the answer is observable.


def _egress(provider: str = "") -> str:
    return client_egress_host(make_async_client(provider))


def _read_timeout(client) -> float:
    """SDK 2.36 stores a float here, 2.38 an httpx.Timeout. Read either."""
    t = client.timeout
    return float(getattr(t, "read", t))


def test_groq_prompts_reach_groq(monkeypatch):
    """The reported defect, stated as the destination rather than the config.

    LLM_PROVIDER=groq with a key present must produce a client pointed at Groq.
    Before the private copies were deleted it produced an OpenAI client, and
    because OPENAI_API_KEY is set in the same deployments, the calls *succeeded*
    — billed to OpenAI, answered by an OpenAI model, logged as provider=groq.
    A green request is not evidence the routing was right.
    """
    monkeypatch.setenv("LLM_PROVIDER", "groq")
    monkeypatch.setenv("GROQ_API_KEY", "gk_test")
    monkeypatch.setenv("OPENAI_API_KEY", "sk_test")
    assert resolve_provider("groq") == "groq"
    assert _egress("groq") == "api.groq.com"


def test_groq_without_a_key_falls_back_to_ollama_not_openai(monkeypatch):
    """Groq is opt-in: no GROQ_API_KEY means local, never someone else's cloud.

    The negative half is the point. An OPENAI_API_KEY is present in nearly every
    deployment, so "not groq" must not quietly mean "openai" — that would send
    schema metadata off-box on a misconfiguration.
    """
    monkeypatch.setenv("LLM_PROVIDER", "groq")
    monkeypatch.setenv("OPENAI_API_KEY", "sk_test")
    host = _egress("groq")
    assert host == "host.docker.internal"
    assert host != "api.openai.com"


@pytest.mark.parametrize(
    ("provider", "env", "expected_host"),
    [
        ("openai", {"OPENAI_API_KEY": "sk_test"}, "api.openai.com"),
        ("ollama", {}, "host.docker.internal"),
        ("groq", {"GROQ_API_KEY": "gk_test"}, "api.groq.com"),
    ],
)
def test_each_provider_reaches_its_own_host(provider, env, expected_host, monkeypatch):
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    assert _egress(provider) == expected_host


@pytest.mark.parametrize(
    ("provider", "env"),
    [
        ("openai", {"OPENAI_API_KEY": "sk_test"}),
        ("ollama", {}),
        ("groq", {"GROQ_API_KEY": "gk_test"}),
        (
            "azure",
            {
                "AZURE_OPENAI_ENDPOINT": "https://acme-prod.openai.azure.com/",
                "AZURE_OPENAI_API_KEY": "az_test",
            },
        ),
    ],
)
def test_every_provider_client_carries_a_bounded_timeout(provider, env, monkeypatch):
    """The SDK default is 600s on read/write/pool, so one stalled upstream call
    parks a Uvicorn worker for ten minutes. That was known — the openai branch
    carried an explicit 120s and a comment saying why — but ollama and groq did
    not, the usual shape of this bug: the fix applied to one arm of the switch.

    The 600 below is not a remembered constant; it is measured from the SDK in
    this same test, so the assertion still means something if the SDK changes it.
    """
    from openai import AsyncOpenAI

    sdk_default = _read_timeout(AsyncOpenAI(api_key="sk_reference"))
    assert sdk_default == 600, (
        f"SDK read-timeout default is now {sdk_default}, not 600 — this test's "
        "premise moved; re-check what an unbounded call costs a worker"
    )

    for k, v in env.items():
        monkeypatch.setenv(k, v)
    got = _read_timeout(make_async_client(provider))
    assert got != sdk_default, f"{provider} client inherits the SDK default timeout"
    assert got == 120.0, f"{provider} client timeout is {got}, expected the shared 120s"


def test_caller_can_widen_the_timeout_without_building_its_own_client(monkeypatch):
    """The parameter exists so a slow caller asks for a window instead of
    hand-rolling an AsyncOpenAI — which is how the branch table got duplicated
    in the first place."""
    monkeypatch.setenv("OPENAI_API_KEY", "sk_test")
    assert _read_timeout(make_async_client("openai", timeout=300.0)) == 300.0


def test_egress_host_reports_azure_without_leaking_the_resource_name(monkeypatch):
    """This value is logged at startup, and the log line promises "no keys, no
    endpoints". An Azure endpoint is the customer's resource name, so azure is
    reported as a marker: the operator still learns the traffic is not going to
    a public host, and the hostname stays out of the logs."""
    monkeypatch.setenv("AZURE_OPENAI_ENDPOINT", "https://acme-prod.openai.azure.com/")
    monkeypatch.setenv("AZURE_OPENAI_API_KEY", "az_test")
    host = _egress("azure")
    assert host == "<azure-endpoint>"
    assert "acme-prod" not in host


def test_egress_host_of_a_mock_deployment_is_not_a_crash(monkeypatch):
    """USE_MOCK_LLM leaves the clients as None and the startup log still runs."""
    assert client_egress_host(None) == "-"
