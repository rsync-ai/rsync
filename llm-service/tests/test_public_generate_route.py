"""``POST /v1/generate`` in the community edition: one contract, two handlers.

Stage 2 of the scaffolder promotion. Stage 1 (#970) moved the deterministic
renderer -- OpenAPI document in, connector out, no model call -- onto the shipped
side of the moat boundary and gave it a CLI. It was still unreachable over HTTP,
so a self-hosted user could only use it by exec'ing into a container. This suite
covers the route that closes that: ``src/lifecycle/scaffold_routes.py``, mounted
by the community entrypoint at the SAME path the cloud service serves.

WHY THE SAME PATH, AND WHY THAT MATTERS HERE. CLAUDE.md's OSS/cloud rule forbids
a runtime ``if edition ==`` branch; the split is which artifact ships. Two
handlers answering one path keeps that true only if they answer the same wire
contract, so ``GenerateRequestV1`` / ``GenerateResponseV1`` live in
``contracts/generate_v1.py`` -- a package BOTH images COPY -- and each handler
imports them. A hand-copied contract is the defect class #970 closed six
instances of: the copy drifts, and the drift is invisible until a field the
api-gateway forwards stops being read by one of the two handlers. The first
group below is what keeps the copy from coming back.

WHY THE REFUSAL BODIES ARE ASSERTED FIELD BY FIELD. A refusal here is the normal
answer to a common request -- the community service cannot read prose docs or
introspect GraphQL -- so the sentence explaining that is the feature, and three
independent readers have to receive it:

  * ``api-gateway/internal/handlers/connector_generator.go:479`` copies ``error``
    into ``error_message`` only when ``error_message`` is ABSENT, and passes the
    upstream status through unchanged (``:491``).
  * ``frontend/src/lib/api/discovery.ts:298`` (the generate call) reads
    ``body?.error || body?.detail``.
  * ``frontend/src/lib/api/discovery.ts:180`` (the generic helper) reads
    ``body?.detail || body?.error``.

Neither frontend reader looks at ``error_message`` at all, and neither reads
``detail`` from a body this route emits. So a refusal that set only
``error_message`` -- the field the response MODEL declares -- would reach the user
as a bare "Bad Request". Both fields are set for that reason, and asserting only
one of them would leave the other free to be dropped.

WHY THERE IS AN IMAGE-TREE GROUP AT THE BOTTOM. The route is only delivered if it
is reachable in the image the quickstart pulls. ``Dockerfile.oss`` is an
allowlist, so a subtree that is needed but not COPY'd is silently absent rather
than loudly leaked, and nothing about that is visible in a source-tree test.
``test_community_image_can_scaffold.py`` asks the same question of the OTHER
image and the OTHER entrypoint (``Dockerfile.community`` + the scaffolder CLI);
this group asks it of ``Dockerfile.oss`` + the HTTP route, which is the artifact
``docker-compose.quickstart.yml`` runs as ``tool-generator``.

THE IMPORT SPELLING IS ``src.…`` THROUGHOUT, deliberately. ``scaffold_routes``
imports the contract as ``src.agents.tool_generator.contracts.generate_v1``;
importing it here under the bare ``agents.…`` spelling that the sibling
``test_gen_*`` suites use would load the same FILE as a second module object with
its own copy of the classes, so ``isinstance`` and ``model_dump`` comparisons
would answer about a different class than the route uses. One spelling, and it
is the route's.
"""

from __future__ import annotations

import ast
import json
import os
import re
import shutil
import subprocess
import sys

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.agents.tool_generator.contracts import generate_v1 as contract
from src.lifecycle.scaffold_routes import scaffold_router

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
LLM_DIR = os.path.normpath(os.path.join(TESTS_DIR, ".."))
SRC_DIR = os.path.join(LLM_DIR, "src")
DOCKERFILE_OSS = os.path.join(LLM_DIR, "Dockerfile.oss")

CONTRACT_MODULE = os.path.join(
    SRC_DIR, "agents", "tool_generator", "contracts", "generate_v1.py"
)
AGENTIC_HANDLER = os.path.join(
    SRC_DIR, "agents", "tool_generator", "agents", "integration.py"
)
DETERMINISTIC_HANDLER = os.path.join(SRC_DIR, "lifecycle", "scaffold_routes.py")

CONTRACT_MODELS = ("GenerateRequestV1", "GenerateResponseV1")

# Two collection paths and one item path. The item route folds into the
# collection, so the converter reports ONE resource -- asserted below, because a
# count that silently became 2 would mean the fold stopped working.
SPEC = json.dumps(
    {
        "openapi": "3.0.0",
        "info": {"title": "Widget API", "version": "1.0", "description": "Widgets."},
        "servers": [{"url": "https://api.example.com/v1"}],
        "components": {
            "securitySchemes": {
                "key": {"type": "apiKey", "in": "header", "name": "X-Api-Key"}
            }
        },
        "security": [{"key": []}],
        "paths": {
            "/widgets": {
                "get": {
                    "operationId": "listWidgets",
                    "responses": {"200": {"description": "ok"}},
                }
            },
            "/widgets/{widgetId}": {
                "get": {
                    "operationId": "getWidget",
                    "responses": {"200": {"description": "ok"}},
                }
            },
        },
    }
)


def _parse(path):
    with open(path, encoding="utf-8") as fh:
        return ast.parse(fh.read(), filename=path)


def _python_files(*roots):
    for root in roots:
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [d for d in dirnames if d != "__pycache__"]
            for fn in sorted(filenames):
                if fn.endswith(".py"):
                    yield os.path.join(dirpath, fn)


# --------------------------------------------------------------------------- #
# One contract, imported by both handlers
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("model", CONTRACT_MODELS)
def test_the_wire_contract_is_defined_exactly_once(model):
    """Two handlers, one definition -- the property the contracts package exists for.

    Walks every module under ``src/`` rather than the two handlers, because the
    way this regresses is someone adding a THIRD definition somewhere convenient
    (a schemas module, a new route file) and importing that instead. Asserting
    only that ``integration.py`` has no copy would not see it.
    """
    definitions = []
    for path in _python_files(SRC_DIR):
        for node in ast.walk(_parse(path)):
            if isinstance(node, ast.ClassDef) and node.name == model:
                definitions.append((os.path.relpath(path, LLM_DIR), node.lineno))

    assert len(definitions) == 1, (
        f"{model} is defined {len(definitions)} times: {definitions}. Both editions "
        "answer /v1/generate on this shape, so a second definition is a copy, and a "
        "copy drifts without anything failing."
    )
    assert definitions[0][0] == os.path.relpath(CONTRACT_MODULE, LLM_DIR), (
        f"{model} is defined in {definitions[0][0]}, not in the shared contracts "
        "package both images COPY"
    )


@pytest.mark.parametrize(
    "handler", [AGENTIC_HANDLER, DETERMINISTIC_HANDLER], ids=["agentic", "deterministic"]
)
def test_both_handlers_import_the_contract_rather_than_restating_it(handler):
    """Each handler must reach the shared module by an import, not by a class body."""
    tree = _parse(handler)

    redefined = [
        n.name
        for n in ast.walk(tree)
        if isinstance(n, ast.ClassDef) and n.name in CONTRACT_MODELS
    ]
    assert not redefined, (
        f"{os.path.relpath(handler, LLM_DIR)} defines {redefined} instead of importing "
        "them from contracts/generate_v1.py"
    )

    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and (node.module or "").endswith(
            "contracts.generate_v1"
        ):
            imported.update(a.name for a in node.names)
    assert set(CONTRACT_MODELS) <= imported, (
        f"{os.path.relpath(handler, LLM_DIR)} imports {sorted(imported)} from the "
        f"contracts package; it needs {list(CONTRACT_MODELS)}"
    )


def test_the_contract_module_imports_nothing_the_community_image_strips():
    """Its whole value is being importable in both images.

    ``contracts/`` is COPY'd by ``Dockerfile.oss``; ``agents/``, ``config/`` and
    ``utils/`` are not. An import added here later would break the community
    service at BOOT, which is the loudest possible place and the latest possible
    one -- after publish.
    """
    allowed = {"typing", "pydantic", "__future__"}
    reached = []
    for node in ast.walk(_parse(CONTRACT_MODULE)):
        if isinstance(node, ast.Import):
            reached.extend(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            # A relative import from inside tool_generator is exactly what must
            # not appear: level > 0 means it reaches back into the package.
            if node.level:
                reached.append(f"<relative>{'.' * node.level}{node.module or ''}")
            else:
                reached.append((node.module or "").split(".")[0])

    offenders = sorted({r for r in reached if r not in allowed})
    assert not offenders, (
        "contracts/generate_v1.py imports "
        f"{offenders}; its closure must stay stdlib typing + pydantic so both "
        "images can import it"
    )


def test_the_deterministic_handler_does_not_compute_a_quality_tier():
    """``quality_tier`` is the cloud fast path's word, and its definition is stripped.

    ``agents/session_fast_path.py`` computes one from spec attributes and does
    not ship in the community image. Restating the rule here would be a second
    definition of the same word -- the copied-contract defect one directory over.
    The frontend already guards on the field being absent, so leaving it unset is
    the honest answer rather than a gap.
    """
    hits = []
    for node in ast.walk(_parse(DETERMINISTIC_HANDLER)):
        if isinstance(node, ast.keyword) and node.arg == "quality_tier":
            hits.append(node.lineno)
        elif isinstance(node, ast.Attribute) and node.attr == "quality_tier":
            hits.append(node.lineno)
        elif isinstance(node, ast.Constant) and node.value == "quality_tier":
            hits.append(node.lineno)

    assert not hits, (
        f"scaffold_routes.py sets quality_tier at line(s) {hits}. The rule that "
        "computes it lives in a module this image strips, so a value set here is a "
        "second definition of the cloud's word."
    )


# --------------------------------------------------------------------------- #
# The route, in process
# --------------------------------------------------------------------------- #


@pytest.fixture(autouse=True)
def _dev_s2s_gate(monkeypatch):
    """Run the in-process suite as the dev compose runs it.

    The S2S gate only lets an unauthenticated call through when ENVIRONMENT is
    explicitly development; an unset ENVIRONMENT now fails closed (503). Set it
    here rather than inherit whatever the shell running pytest happens to have.
    """
    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.delenv("INTERNAL_SERVICE_SECRET", raising=False)


@pytest.fixture(scope="module")
def client():
    """The router mounted exactly as ``src/lifecycle/main.py`` mounts it."""
    app = FastAPI()
    app.include_router(scaffold_router, prefix="/v1")
    return TestClient(app)


def test_the_community_entrypoint_serves_generate():
    """The mount itself. Without this line the route exists and nothing reaches it.

    Read off the OpenAPI document rather than ``app.routes``: on fastapi 0.137
    an included router appears there as a ``_IncludedRouter`` with no ``.path``,
    so enumerating routes would find the endpoint missing whether or not it is.
    """
    import src.lifecycle.main as entrypoint

    paths = entrypoint.app.openapi()["paths"]
    assert "/v1/generate" in paths, (
        "the community entrypoint does not serve /v1/generate, so the api-gateway's "
        f"TOOL_GENERATOR_URL has nothing to call. It serves: {sorted(paths)}"
    )
    assert "post" in paths["/v1/generate"], sorted(paths["/v1/generate"])
    # The route it shares the service with must not have been displaced.
    assert "/v1/deploy" in paths and "/health" in paths, sorted(paths)


REFUSALS = [
    pytest.param(
        {"api_name": "x", "openapi_spec_url": "http://169.254.169.254/spec.json"},
        "does not fetch specifications by URL",
        id="spec_url_is_an_ssrf_primitive",
    ),
    pytest.param(
        {"api_name": "x", "graphql_endpoint": "https://example.com/graphql"},
        "OpenAPI and Swagger documents only",
        id="graphql_endpoint",
    ),
    pytest.param(
        {"api_name": "x", "graphql_schema": '{"__schema": {}}'},
        "OpenAPI and Swagger documents only",
        id="graphql_schema",
    ),
    pytest.param(
        {"api_name": "x", "session_id": "sess-1"},
        "/v1/discover",
        id="discovery_session",
    ),
    pytest.param(
        {"api_name": "x", "docs_url": "https://example.com/docs"},
        "agentic",
        id="docs_url",
    ),
    pytest.param(
        {"api_name": "x", "docs_text": "GET /things returns a list"},
        "agentic",
        id="docs_text",
    ),
    pytest.param({"api_name": "x"}, "No OpenAPI document was supplied", id="no_input"),
]


@pytest.mark.parametrize("payload,fragment", REFUSALS)
def test_an_unsupported_input_is_refused_readably(client, payload, fragment):
    """400, with the reason in every field the three readers actually read."""
    res = client.post("/v1/generate", json=payload)
    assert res.status_code == 400, res.text
    body = res.json()

    assert body["success"] is False
    assert body["status"] == "refused"
    assert body["error_stage"] == "input_validation"
    assert fragment in body["error_message"], body["error_message"]
    # The api-gateway forwards this body verbatim, so `error` is what both
    # frontend readers see. Equal, not merely present: a divergence would mean
    # the UI and an API client are told different things about one refusal.
    assert body["error"] == body["error_message"]


def test_a_refusal_names_something_the_caller_can_do_instead(client):
    """An error that only says no is a dead end for a self-hosted user."""
    body = client.post("/v1/generate", json={"api_name": "x"}).json()
    assert body["suggestions"], "the no-input refusal offers no next step"
    assert any("openapi" in s.lower() for s in body["suggestions"]), body["suggestions"]


def test_an_unparseable_document_is_refused_at_the_conversion_stage(client):
    """Distinct stage from an unsupported input: the caller sent the right FIELD."""
    res = client.post(
        "/v1/generate", json={"api_name": "x", "openapi_spec": "<html>nope</html>"}
    )
    assert res.status_code == 400, res.text
    body = res.json()
    assert body["error_stage"] == "spec_conversion", body
    assert body["error"] == body["error_message"]


def test_a_dry_run_renders_a_connector_and_writes_nothing(client, tmp_path):
    """The generate path end to end, with persistence off.

    ``save_artifacts=false`` is the regression shape the private handler already
    honours, and it is what makes this suite runnable in CI: the persist branch
    calls ``DeploymentService.deploy``, which writes into TOOLS_DIR and, in
    managed mode, talks to docker.sock.
    """
    res = client.post(
        "/v1/generate",
        json={"api_name": "widget_api", "openapi_spec": SPEC, "save_artifacts": False},
    )
    assert res.status_code == 200, res.text
    body = res.json()

    assert body["success"] is True
    assert body["status"] == "completed_dry_run"
    assert body["error_message"] is None

    # The converter slugs the name and the schema derives the class, so both come
    # from the spec object rather than from the raw request.
    assert body["connector_name"] == "widget-api"
    assert body["class_name"] == "WidgetApiConnector"
    assert body["protocol"] == "rest"
    assert body["operation_count"] == 1, "the item route should fold into /widgets"

    assert body["quality_tier"] is None, (
        "quality_tier came back set; the module that defines it is stripped from "
        "this image"
    )
    assert body["metadata"]["generation_mode"] == "deterministic_openapi"
    assert body["code_preview"], "no code was rendered"
    assert [s["name"] for s in body["workflow_stages"]] == ["convert", "render"], (
        "a dry run must stop before persist"
    )
    # Nothing on disk: the response names no output path and no version.
    assert body["output_path"] is None and body["version"] is None, body


def test_a_category_the_scaffolder_cannot_render_is_noted_not_refused(client):
    """`category` is a hint on this route. Refusing on it would fail a working request."""
    body = client.post(
        "/v1/generate",
        json={
            "api_name": "widget_api",
            "openapi_spec": SPEC,
            "save_artifacts": False,
            "category": "not_a_category",
        },
    ).json()
    assert body["success"] is True, body
    assert any("not_a_category" in n for n in body["metadata"]["notes"]), body["metadata"]


def test_the_route_is_behind_the_same_s2s_gate_as_deploy(monkeypatch):
    """It builds and starts containers over docker.sock in managed mode.

    Same dependency as ``/v1/deploy`` and the same reason. The gate lets a call
    through without a header only when ``INTERNAL_SERVICE_SECRET`` is unset AND
    ``ENVIRONMENT`` is development (the autouse fixture above), which is why the
    rest of this suite needs no header -- and why the secret has to be set here
    to see the gate at all.
    """
    monkeypatch.setenv("INTERNAL_SERVICE_SECRET", "FAKEPLACEHOLDER-s2s")
    app = FastAPI()
    app.include_router(scaffold_router, prefix="/v1")
    gated = TestClient(app)

    unauthenticated = gated.post("/v1/generate", json={"api_name": "x"})
    assert unauthenticated.status_code == 401, unauthenticated.text

    wrong = gated.post(
        "/v1/generate",
        json={"api_name": "x"},
        headers={"X-Internal-Secret": "FAKEPLACEHOLDER-wrong"},
    )
    assert wrong.status_code == 401, wrong.text

    # With the secret the request reaches the handler, which then refuses it on
    # its merits (no spec) rather than on authentication.
    allowed = gated.post(
        "/v1/generate",
        json={"api_name": "x"},
        headers={"X-Internal-Secret": "FAKEPLACEHOLDER-s2s"},
    )
    assert allowed.status_code == 400, allowed.text
    assert allowed.json()["error_stage"] == "input_validation"


# --------------------------------------------------------------------------- #
# The route inside a tree holding exactly what Dockerfile.oss ships
# --------------------------------------------------------------------------- #

# Only the trees the image is an allowlist over. `requirements-oss.txt` is COPY'd
# too and says nothing about whether the Python closes.
_PARTITIONED = ("src", "prompts")

# Every subtree below is COPY'd by Dockerfile.oss AND required for a generate
# request to return 200. That is measured, not assumed: `test_the_simulation_can_fail`
# deletes each one in turn and requires the render to stop working. Four of the
# seven are reached only from inside the request handler, so deleting them leaves
# `import src.lifecycle.main` succeeding -- which is exactly why the probe below
# issues a request instead of importing and stopping.
_REQUIRED_SUBTREES = [
    "contracts",
    "deployment",
    "generator",
    "scaffold",
    "schemas",
    "templates",
    "validation",
]

# Runs inside the materialised tree with PYTHONPATH pointing at it and nothing
# else first-party on the path. Prints one machine-readable line so a crash and a
# wrong answer cannot be confused for each other.
_ROUTE_PROBE = r"""
import json, sys
from fastapi.testclient import TestClient

import src.lifecycle.main as entrypoint

spec = json.dumps({
    "openapi": "3.0.0",
    "info": {"title": "Widget API", "version": "1.0"},
    "servers": [{"url": "https://api.example.com"}],
    "paths": {"/widgets": {"get": {"operationId": "listWidgets",
                                   "responses": {"200": {"description": "ok"}}}}},
})
res = TestClient(entrypoint.app).post(
    "/v1/generate",
    json={"api_name": "widget_api", "openapi_spec": spec, "save_artifacts": False},
)
body = res.json()
moat = sorted(
    name for name in sys.modules
    if name.startswith("src.agents.tool_generator.")
    and name.split(".")[3] in ("agents", "config", "utils", "harness", "mock_server")
)
print("PROBE " + json.dumps({
    "paths": sorted(entrypoint.app.openapi()["paths"]),
    "http_status": res.status_code,
    "status": body.get("status"),
    "class_name": body.get("class_name"),
    "error_message": body.get("error_message"),
    "moat_modules": moat,
}))
"""


def _oss_copy_sources():
    """Source paths named by Dockerfile.oss COPY lines.

    Same parse as the boundary guard's, kept local for the same reason
    `test_community_image_can_scaffold.py` keeps its own: a regex loosened for
    one file's benefit must not quietly change another file's subject.
    """
    srcs = []
    with open(DOCKERFILE_OSS, encoding="utf-8") as fh:
        for line in fh:
            m = re.match(r"^\s*COPY\s+(?!--)(\S+)\s+(\S+)\s*$", line)
            if m:
                srcs.append(m.group(1).rstrip("/"))
    return [s for s in srcs if s.split("/")[0] in _PARTITIONED]


def _materialise(dest):
    copied = 0
    for rel in _oss_copy_sources():
        src = os.path.join(LLM_DIR, rel)
        dst = os.path.join(dest, rel)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        if os.path.isdir(src):
            shutil.copytree(
                src,
                dst,
                dirs_exist_ok=True,
                ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
            )
        else:
            shutil.copy2(src, dst)
        copied += 1
    return copied


def _probe(tree):
    """`env -i`-equivalent: no inherited PYTHONPATH can put the stripped tree back."""
    return subprocess.run(
        [sys.executable, "-c", _ROUTE_PROBE],
        cwd=tree,
        env={
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "HOME": tree,
            "PYTHONPATH": tree,
            "PYTHONDONTWRITEBYTECODE": "1",
            "OTEL_SDK_DISABLED": "true",
            # The probe checks what the image imports, not auth; without this the
            # S2S gate answers 503 (an unset ENVIRONMENT fails closed).
            "ENVIRONMENT": "development",
        },
        capture_output=True,
        text=True,
    )


def _probe_result(proc):
    lines = [ln for ln in proc.stdout.splitlines() if ln.startswith("PROBE ")]
    return json.loads(lines[-1][len("PROBE ") :]) if lines else None


@pytest.fixture(scope="module")
def oss_image_tree(tmp_path_factory):
    tree = str(tmp_path_factory.mktemp("oss-image"))
    copied = _materialise(tree)
    # Vacuity guard: a COPY parse that matched nothing would hand every assertion
    # below an empty tree to be trivially wrong about.
    assert copied >= 10, f"Dockerfile.oss COPY parse yielded {copied} src/prompts paths"
    return tree


def test_the_lifecycle_image_serves_a_generate_request(oss_image_tree):
    """The delivery claim, against the allowlist rather than the source tree.

    A moat module missing from the image is what makes this different from the
    in-process tests above: those run against the full checkout, where every
    stripped path is present and an accidental dependency on one is invisible.
    """
    proc = _probe(oss_image_tree)
    result = _probe_result(proc)
    assert result is not None, (
        "the probe produced no result line inside a tree holding exactly what "
        f"Dockerfile.oss COPYs.\n--- stdout ---\n{proc.stdout}\n--- stderr ---\n"
        f"{proc.stderr}"
    )

    assert "/v1/generate" in result["paths"], result["paths"]
    assert result["http_status"] == 200, result
    assert result["status"] == "completed_dry_run", result
    assert result["class_name"] == "WidgetApiConnector", result
    assert not result["moat_modules"], (
        "the community entrypoint loaded moat modules: "
        f"{result['moat_modules']}. They are absent from the image, so this would "
        "be a boot failure there."
    )


@pytest.mark.parametrize("subtree", _REQUIRED_SUBTREES)
def test_the_simulation_can_fail(oss_image_tree, tmp_path, subtree):
    """Delete one COPY'd subtree and the request must stop succeeding.

    The denominator for the test above. Without it, a probe that had quietly
    stopped exercising the renderer would pass exactly as loudly as a correct
    one -- a failure mode this repo has shipped before. Each entry is a directory
    ``Dockerfile.oss`` names; if one stops being required that is a real change
    in what the image needs, and this list should shrink deliberately rather than
    be discovered when the positive test has stopped proving anything.
    """
    tree = str(tmp_path / f"without-{subtree}")
    shutil.copytree(oss_image_tree, tree)
    victim = os.path.join(tree, "src", "agents", "tool_generator", subtree)
    assert os.path.isdir(victim), f"{subtree}/ is not in the image tree to begin with"
    shutil.rmtree(victim)

    result = _probe_result(_probe(tree))
    rendered = result is not None and result["status"] == "completed_dry_run"
    assert not rendered, (
        f"/v1/generate still rendered a connector with {subtree}/ deleted, so this "
        "simulation is not exercising it and the positive test above proves less "
        "than it appears to."
    )
