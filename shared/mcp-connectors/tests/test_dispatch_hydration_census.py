"""Census gate: every vendored `_handle_tool_call` hydrates connection config.

THE CENSUS (KI-KAFKA-SINK-NO-CONFIG-HYDRATION, resolved):

    DENOMINATOR: 23 · hydrates: 22 · does not: 1

Connectors run from `versions/<current_version>/`, not from the canonical
`shared/mcp-connectors/base_connector.py`, so each vendored copy carries its own
`_handle_tool_call`. The canonical one opens with a step-0 block that calls
`self.configure_from_connection(tool_args["config"])`, which is what makes
OAuth/API-key connectors work generically. A vendored copy that silently lacks it
has an unreachable auth path.

THE ONE EXEMPTION -- `internal/kafka-mcp-sink` -- is deliberate, not drift. For
that connector `arguments.config` is the SINK config declared in its
metadata.json `config_schema` (topics / consumer_group / destination_connector /
destination_config), built at
`backend-orchestrator/internal/agents/executor/executor.go:6216` and forwarded as
`arguments` by `backend-orchestrator/internal/mcp/client.go:292`. It is
`internal: true` / `auth_type: none` with empty required_config, and its Kafka
SASL/TLS comes from the environment. Passing that dict to the auth hydrator would
be type confusion, not symmetry. The reasoning is restated at the call site
itself; test (7) below keeps it there.

WHY THIS FILE EXISTS: the census above was originally a hand-run repro pasted into
a known-issues entry. Prose cannot fail. This file makes the sweep executable, so
the question is answered by CI rather than re-derived by the next person who
notices the asymmetry -- and, via the exemption tests, so the exemption cannot
outlive the premise it rests on.

Pure `ast` + `git ls-files`: imports no connector module, needs no DB driver.
"""

import ast
import json
import os
import subprocess

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
# Three levels: tests/ -> mcp-connectors/ -> shared/ -> repo root. Everything
# below is repo-root-relative, and CI runs pytest with cwd=llm-service, so paths
# are derived from __file__ and every git call passes cwd=_REPO.
_REPO = os.path.abspath(os.path.join(_HERE, "..", "..", ".."))

CANONICAL = "shared/mcp-connectors/base_connector.py"

# Floor, not the exact count: new connectors are expected. Guards against a
# discovery step that silently finds nothing (see test_discovery_is_not_vacuous).
MIN_BASE_CONNECTORS = 20

_KAFKA_SINK = "shared/mcp-connectors/internal/kafka-mcp-sink"
_KAFKA_SINK_BASE = _KAFKA_SINK + "/versions/v1.0.0/base_connector.py"

HYDRATION_EXEMPT = {
    _KAFKA_SINK_BASE: (
        "internal sink, auth_type none: arguments.config is the SINK config per "
        "metadata.json config_schema + executor.go:6216, not a connection config "
        "-- see KI-KAFKA-SINK-NO-CONFIG-HYDRATION"
    ),
}

# The marker the call-site comment must keep, so the reasoning stays findable.
_MARKER = "KI-KAFKA-SINK-NO-CONFIG-HYDRATION"


def _tracked_base_connectors():
    """Every tracked base_connector.py, via git -- never a glob.

    `git ls-files` keeps untracked scratch connectors under
    `shared/mcp-connectors/public/` invisible to the census, and `check=True`
    turns "not a checkout" into an error rather than an empty, green sweep.
    """
    out = subprocess.run(
        ["git", "ls-files", "-z", "*base_connector.py"],
        cwd=_REPO,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return sorted(p for p in out.split("\0") if p)


BASE_CONNECTORS = _tracked_base_connectors()


def _handle_tool_call_node(rel_path):
    """The `_handle_tool_call` FunctionDef in `rel_path`, or None."""
    with open(os.path.join(_REPO, rel_path), "r", encoding="utf-8") as fh:
        source = fh.read()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.FunctionDef) and node.name == "_handle_tool_call":
            return node
    return None


def _hydrates(node):
    """True iff the function body calls `<something>.configure_from_connection(...)`."""
    return any(
        isinstance(call, ast.Call)
        and isinstance(call.func, ast.Attribute)
        and call.func.attr == "configure_from_connection"
        for call in ast.walk(node)
    )


def _resolve_current_version_dir(connector_rel):
    """Resolve `versions/<current_version>/` from the connector's latest.json.

    Never hardcode a version: the repo rule is that `latest.json.current_version`
    -- not the highest directory -- names the code that actually ships.
    """
    latest_path = os.path.join(_REPO, connector_rel, "latest.json")
    with open(latest_path, "r", encoding="utf-8") as fh:
        current = json.load(fh)["current_version"]
    return os.path.join(_REPO, connector_rel, "versions", current)


# --- 1. the sweep must actually sweep something ------------------------------

def test_discovery_is_not_vacuous():
    """A census that inspected zero files must not report success.

    This repo has already shipped a green test covering a fiction: a CDC loader
    found 0 of 28 connectors for months while a test exercised that path
    successfully, because a count of zero is not an error.
    """
    assert len(BASE_CONNECTORS) >= MIN_BASE_CONNECTORS, (
        f"discovered only {len(BASE_CONNECTORS)} base_connector.py files "
        f"(floor {MIN_BASE_CONNECTORS}). Either the pathspec regressed or this "
        f"ran outside a git checkout -- the sweep below would pass having checked "
        f"nothing. Found: {BASE_CONNECTORS}"
    )
    assert CANONICAL in BASE_CONNECTORS, (
        f"{CANONICAL} is missing from the discovered set; without the canonical "
        f"file the positive control below cannot run."
    )


# --- 2. a rename must not hollow the census out ------------------------------

@pytest.mark.parametrize("rel_path", BASE_CONNECTORS)
def test_every_base_connector_defines_the_dispatch_hook(rel_path):
    assert _handle_tool_call_node(rel_path) is not None, (
        f"{rel_path} defines no `_handle_tool_call`. If the dispatch hook was "
        f"renamed, update this census -- otherwise every hydration assertion "
        f"below silently stops applying to this file."
    )


# --- 3. positive control: the detector still detects -------------------------

def test_canonical_base_hydrates():
    """If this fails the detector is broken, not the connectors."""
    node = _handle_tool_call_node(CANONICAL)
    assert node is not None, f"{CANONICAL} defines no `_handle_tool_call`"
    assert _hydrates(node), (
        f"{CANONICAL} no longer calls configure_from_connection() from "
        f"`_handle_tool_call`. Either the canonical step-0 block was removed (a "
        f"real defect) or `_hydrates()` no longer matches it -- in which case "
        f"every assertion in this file has been passing vacuously."
    )


# --- 4. the sweep itself -----------------------------------------------------

@pytest.mark.parametrize(
    "rel_path", [p for p in BASE_CONNECTORS if p not in HYDRATION_EXEMPT]
)
def test_every_vendored_dispatch_hydrates_connection_config(rel_path):
    node = _handle_tool_call_node(rel_path)
    assert node is not None, f"{rel_path} defines no `_handle_tool_call`"
    assert _hydrates(node), (
        f"{rel_path}: `_handle_tool_call` never calls configure_from_connection(), "
        f"so this connector's auth path is unreachable -- OAuth/API-key tokens in "
        f"the encrypted connection `config` are silently ignored. Copy the step-0 "
        f"block from {CANONICAL} (`# 0) Configure auth context from connection "
        f"config`), which reads tool_args['config'] and hydrates best-effort. If "
        f"the omission is deliberate, it needs an entry in HYDRATION_EXEMPT plus a "
        f"reasoned comment at the call site -- not a silent gap."
    )


# --- 5. negative control: the exemption cannot outlive its reason ------------

@pytest.mark.parametrize("rel_path", sorted(HYDRATION_EXEMPT))
def test_the_exemption_is_still_earned(rel_path):
    """An exemption that is no longer true is worse than the original bug."""
    assert os.path.exists(os.path.join(_REPO, rel_path)), (
        f"HYDRATION_EXEMPT names {rel_path}, which does not exist. Drop the stale "
        f"entry."
    )
    assert rel_path in BASE_CONNECTORS, (
        f"HYDRATION_EXEMPT names {rel_path}, which the sweep does not discover "
        f"(untracked or moved). A dangling exemption excuses nothing and hides the "
        f"real path."
    )
    node = _handle_tool_call_node(rel_path)
    assert node is not None, f"{rel_path} defines no `_handle_tool_call`"
    assert not _hydrates(node), (
        f"{rel_path} NOW calls configure_from_connection(), but is still listed in "
        f"HYDRATION_EXEMPT ({HYDRATION_EXEMPT[rel_path]}). If the hydration was "
        f"added deliberately, delete the HYDRATION_EXEMPT entry AND the "
        f"{_MARKER} comment at the call site together -- leaving the exemption "
        f"behind permanently excuses a file that no longer needs excusing."
    )


# --- 6. the premise the exemption rests on -----------------------------------

def test_the_exemption_premise_still_holds():
    """kafka-mcp-sink is exempt only because it is never user-connection-configured.

    The day it grows a real auth surface, the exemption becomes a bug and this
    goes red.
    """
    version_dir = _resolve_current_version_dir(_KAFKA_SINK)
    with open(os.path.join(version_dir, "metadata.json"), "r", encoding="utf-8") as fh:
        meta = json.load(fh)

    why = (
        "The KI-KAFKA-SINK-NO-CONFIG-HYDRATION exemption assumes this connector "
        "carries no user connection config, so there is nothing for "
        "configure_from_connection() to hydrate. That assumption has changed: add "
        "the canonical step-0 block to `_handle_tool_call` and remove the "
        "HYDRATION_EXEMPT entry."
    )
    assert meta.get("internal") is True, f"internal is {meta.get('internal')!r}. {why}"
    assert meta.get("auth_type") == "none", (
        f"auth_type is {meta.get('auth_type')!r}. {why}"
    )
    assert meta.get("required_config") == [], (
        f"required_config is {meta.get('required_config')!r}. {why}"
    )


# --- 7. the reasoning must stay at the call site -----------------------------

@pytest.mark.parametrize("rel_path", sorted(HYDRATION_EXEMPT))
def test_call_site_documents_the_divergence(rel_path):
    """The exemption is only defensible if the next reader finds the argument."""
    with open(os.path.join(_REPO, rel_path), "r", encoding="utf-8") as fh:
        source = fh.read()
    node = None
    for candidate in ast.walk(ast.parse(source)):
        if (
            isinstance(candidate, ast.FunctionDef)
            and candidate.name == "_handle_tool_call"
        ):
            node = candidate
            break
    assert node is not None, f"{rel_path} defines no `_handle_tool_call`"

    segment = ast.get_source_segment(source, node)
    assert segment is not None, f"could not extract `_handle_tool_call` from {rel_path}"
    assert _MARKER in segment, (
        f"{rel_path}: `_handle_tool_call` no longer contains the literal "
        f"{_MARKER}. That comment is the only thing telling the next sweeper the "
        f"missing hydration is deliberate; without it this gets re-investigated "
        f"and probably 'fixed' into a type confusion. Restore it, or -- if the "
        f"exemption is genuinely gone -- remove the HYDRATION_EXEMPT entry too."
    )
