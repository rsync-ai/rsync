"""The deterministic scaffolder actually renders, and does it without a model.

`test_scaffold_openapi_conversion.py` checks what the converter DECIDES. This
file checks that those decisions survive into a connector on disk, and that the
whole path stays offline.

The offline claim is the product claim -- the public scaffolder's entire reason
to exist is that it needs no API key, no account and no network -- so it is
asserted the only way that can't drift: a subprocess renders a connector for
real, then reports every module it loaded. Two properties of this package make
the weaker in-process version of that test worthless:

  * pytest has already imported most of the world by the time any test body
    runs, so an in-process `sys.modules` census cannot attribute an import to
    the code under test.
  * `generator/builder.py` inserts its own parent directory on `sys.path` when
    a package-relative import fails, which makes `utils`, `agents` and `config`
    importable under BARE top-level names. A census keyed on module names would
    read `utils.logo_downloader` as an innocent third-party `utils`. Every
    assertion here is therefore keyed on a module's resolved `__file__`.

The census also needs a positive denominator. A child that crashed on its first
import would report a beautifully clean module list, so the parent additionally
requires that the builder WAS loaded and that a non-empty `connector.py` reached
disk before it believes any of the negative assertions.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap

import pytest

# Must run before the first `agents.*` import below: the sibling `test_gen_*.py`
# suites are collected first and leave `agents` bound to the generation
# package's own inner `agents/`, under which `agents.tool_generator` does not
# exist. `_real_agents_package` explains the whole mechanism.
from _real_agents_package import make_agents_tool_generator_importable

make_agents_tool_generator_importable()

from agents.tool_generator.scaffold import openapi_to_spec as converter  # noqa: E402
from agents.tool_generator.scaffold.cli import (  # noqa: E402
    _import_builder,
    load_document,
    run,
    write_artifacts,
)
from agents.tool_generator.scaffold.openapi_to_spec import (  # noqa: E402
    OpenAPIConversionError,
    openapi_to_connector_spec,
)


# `.../src/agents/tool_generator/scaffold/openapi_to_spec.py`
_SCAFFOLD_DIR = os.path.dirname(os.path.realpath(converter.__file__))
_TOOL_GENERATOR_DIR = os.path.dirname(_SCAFFOLD_DIR)
_SRC_DIR = os.path.dirname(os.path.dirname(_TOOL_GENERATOR_DIR))

# Directories inside the generation package that reach an LLM, directly or by
# way of a package `__init__`. `utils/__init__.py` eagerly imports
# `llm_consensus`, which imports `openai`, so the whole package is a barrier.
_LLM_BEARING_SUBTREES = ("agents", "config", "utils")

# Top-level distributions that only exist to talk to a model.
_LLM_SDKS = frozenset({
    "openai", "anthropic", "litellm", "cohere", "vertexai", "tiktoken",
    "google.generativeai", "langchain", "llama_index",
})


def _doc(schemes=None, **overrides):
    doc = {
        "openapi": "3.0.3",
        "info": {"title": "Widget Co API"},
        "servers": [{"url": "https://api.widgetco.test/v2"}],
        "paths": {"/widgets": {"get": {"responses": {"200": {}}}}},
    }
    if schemes is not None:
        doc["components"] = {"securitySchemes": schemes}
    doc.update(overrides)
    return doc


def _render(schemes=None, **kwargs):
    """Convert and render in-process. Returns the GeneratedConnector."""
    spec = openapi_to_connector_spec(_doc(schemes), **kwargs).spec
    return _import_builder()().build_from_dict(spec), spec


# --------------------------------------------------------------------------- #
# The offline guarantee
# --------------------------------------------------------------------------- #

_CENSUS_CHILD = textwrap.dedent(
    """
    import json, os, sys

    src, out_dir, doc_json = sys.argv[1], sys.argv[2], sys.argv[3]
    sys.path.insert(0, src)

    from agents.tool_generator.scaffold.openapi_to_spec import openapi_to_connector_spec
    from agents.tool_generator.scaffold.cli import _import_builder, write_artifacts

    report = openapi_to_connector_spec(json.loads(doc_json))
    generated = _import_builder()().build_from_dict(report.spec)
    written = write_artifacts(generated, report.spec, out_dir)

    census = {}
    for name, module in list(sys.modules.items()):
        path = getattr(module, "__file__", None)
        census[name] = os.path.realpath(path) if path else None

    sys.stdout.write(
        "@@CENSUS@@"
        + json.dumps({"modules": census, "written": sorted(os.path.basename(w) for w in written)})
    )
    """
)


@pytest.fixture(scope="module")
def census(tmp_path_factory):
    """Render a connector in a clean interpreter and report what it imported."""
    workdir = tmp_path_factory.mktemp("census")
    child = workdir / "child.py"
    child.write_text(_CENSUS_CHILD, encoding="utf-8")
    out_dir = workdir / "connector"

    doc = _doc({"k": {"type": "apiKey", "in": "header", "name": "X-Api-Key"}})
    result = subprocess.run(
        [sys.executable, str(child), _SRC_DIR, str(out_dir), json.dumps(doc)],
        capture_output=True,
        text=True,
        timeout=300,
        # A hermetic environment: no inherited PYTHONPATH, no site-packages
        # shortcuts, and no credentials that could make a network call succeed.
        env={"PATH": os.environ.get("PATH", ""), "HOME": str(workdir)},
    )
    assert result.returncode == 0, f"child failed:\n{result.stdout}\n{result.stderr}"
    marker = result.stdout.rindex("@@CENSUS@@") + len("@@CENSUS@@")
    payload = json.loads(result.stdout[marker:])
    payload["out_dir"] = out_dir
    return payload


def test_the_census_child_really_rendered_a_connector(census):
    """The denominator. Without this, an early crash reads as a clean census."""
    assert "agents.tool_generator.generator.builder" in census["modules"]
    assert "connector.py" in census["written"]
    body = (census["out_dir"] / "connector.py").read_text(encoding="utf-8")
    assert len(body.splitlines()) > 200


def test_scaffold_is_llm_free(census):
    """Rendering a connector must not load any part of the agentic pipeline.

    Keyed on each module's resolved file, not its name: `builder.py` can put
    `utils` and `config` on `sys.path` as bare top-level packages, so a
    name-based check would miss exactly the imports that matter.
    """
    forbidden = [
        os.path.join(_TOOL_GENERATOR_DIR, subtree) + os.sep
        for subtree in _LLM_BEARING_SUBTREES
    ]
    offenders = sorted(
        f"{name} ({path})"
        for name, path in census["modules"].items()
        if path and any(path.startswith(prefix) for prefix in forbidden)
    )
    assert offenders == [], "rendering reached the agentic tree: " + "; ".join(offenders)


def test_no_model_sdk_is_loaded_while_rendering(census):
    loaded = {name.split(".")[0] for name in census["modules"]} | set(census["modules"])
    assert not (loaded & _LLM_SDKS), sorted(loaded & _LLM_SDKS)


def test_the_census_is_capable_of_failing(census):
    """Prove the file-prefix match discriminates, rather than never matching.

    A prefix built from a directory the render genuinely uses must produce
    offenders. Without this control, a typo in `_TOOL_GENERATOR_DIR` would make
    `test_scaffold_is_llm_free` vacuously green forever.
    """
    control = os.path.join(_TOOL_GENERATOR_DIR, "generator") + os.sep
    matched = [p for p in census["modules"].values() if p and p.startswith(control)]
    assert matched, "the prefix comparison never matches anything, so it proves nothing"


# --------------------------------------------------------------------------- #
# The rendered connector's auth wiring
# --------------------------------------------------------------------------- #

def test_a_rendered_connector_is_syntactically_valid_python():
    generated, _ = _render()
    compile(generated.code, "connector.py", "exec")


def test_an_api_key_header_is_sent_without_a_bearer_prefix():
    """The end of the trail the converter's `header_prefix: ""` starts.

    `AuthConfig.header_prefix` defaults to "Bearer" for every auth type, and the
    template treats that default as unset. Dropping the explicit empty string in
    the converter therefore changes nothing in the spec's shape and everything
    in the emitted code -- which is why this asserts on the rendered line.
    """
    generated, _ = _render({"k": {"type": "apiKey", "in": "header", "name": "X-Api-Key"}})
    assert 'header_name = "X-Api-Key"' in generated.code
    assert 'prefix = "".strip()' in generated.code


def test_a_query_api_key_is_injected_into_the_request_parameters():
    generated, _ = _render({"k": {"type": "apiKey", "in": "query", "name": "api_token"}})
    assert '"api_token": _tok' in generated.code


def test_a_bearer_token_goes_in_the_authorization_header():
    generated, _ = _render({"k": {"type": "http", "scheme": "bearer"}})
    assert 'header_name = "Authorization"' in generated.code


def test_basic_auth_reads_both_credentials_from_the_environment():
    generated, _ = _render({"k": {"type": "http", "scheme": "basic"}})
    assert "os.getenv('MCP_USERNAME'" in generated.code
    assert "os.getenv('MCP_PASSWORD'" in generated.code
    assert '"Authorization": f"Basic {credentials}"' in generated.code


def test_an_undeclared_auth_still_renders_an_optional_key_header():
    """A document silent about auth is not proof the API is public."""
    generated, _ = _render()
    assert 'headers["X-API-Key"] = token' in generated.code


# --------------------------------------------------------------------------- #
# The rendered connector's metadata declares its auth methods
# --------------------------------------------------------------------------- #

# Every credentialed scheme the converter can choose, and the method id the
# metadata must declare for it. `llm-service/tests/test_connector_auth_contract.py`
# enforces the same rule over the connectors already on disk; this is the same
# rule applied at the moment a NEW one is rendered, so a scaffolded connector
# cannot reach disk in the state that would fail it.
_CREDENTIALED_SCHEMES = [
    ({"k": {"type": "http", "scheme": "bearer"}}, "bearer"),
    ({"k": {"type": "apiKey", "in": "header", "name": "X-Api-Key"}}, "api_key"),
    ({"k": {"type": "apiKey", "in": "query", "name": "api_token"}}, "api_key_query"),
    ({"k": {"type": "http", "scheme": "basic"}}, "basic"),
    ({"k": {"type": "oauth2", "flows": {}}}, "bearer"),
]


@pytest.mark.parametrize(
    "schemes,expected_method",
    _CREDENTIALED_SCHEMES,
    ids=[m for _, m in _CREDENTIALED_SCHEMES[:-1]] + ["oauth2_downgraded"],
)
def test_a_credentialed_connector_declares_its_auth_method(schemes, expected_method):
    """The converter builds a single flat auth dict and never sets
    `supported_methods`, so the array in metadata.json exists only because
    `AuthConfig.declared_methods` derives it. Without the derivation the
    connection modal has to guess which config key carries the credential.
    """
    generated, _ = _render(schemes)
    meta = json.loads(generated.metadata)

    assert meta["auth_type"] != "none"
    methods = meta.get("supported_auth_methods")
    assert isinstance(methods, list) and methods, (
        f"{meta['auth_type']} connector declared no auth methods: {methods!r}"
    )
    assert [m["method"] for m in methods] == [expected_method]
    assert methods[0]["config_keys"], "a declared method with no config key names nothing"


def test_a_public_api_declares_no_auth_method():
    """auth_type "none" is the one exemption the contract grants, and the
    converter reaches it whenever a document declares no usable scheme.
    """
    generated, spec = _render()
    assert spec["auth"]["type"] == "none"
    assert "supported_auth_methods" not in json.loads(generated.metadata)


def test_declaring_a_method_does_not_move_the_connector_off_the_legacy_auth_path():
    """The derivation is a metadata-only view, and this is what that buys.

    `connector.py.j2` switches to a multi-auth dispatcher whenever
    `spec.auth.supported_methods` is non-empty, and that dispatcher sets the
    auth header unconditionally -- where the legacy branch deliberately omits an
    empty `Authorization: Bearer`, which 401s on public endpoints. Deriving the
    array from the flat fields instead of writing it back into the spec is what
    keeps the rendered code on the legacy branch.
    """
    generated, spec = _render({"k": {"type": "http", "scheme": "bearer"}})
    assert spec["auth"].get("supported_methods") in (None, [])
    assert json.loads(generated.metadata)["supported_auth_methods"]
    # Keyed on the two constructs the dispatcher branch OWNS, not on the bare
    # names: the legacy branch still reads `SUPPORTED_AUTH_METHODS` defensively
    # through `getattr`, so the name alone does not discriminate the branches.
    assert "SUPPORTED_AUTH_METHODS = {" not in generated.code
    assert "def _resolve_auth_method" not in generated.code
    # The positive half -- the legacy bearer branch is what actually rendered,
    # and it is the branch that omits the header when no token is configured.
    assert 'header_name = "Authorization"' in generated.code


def test_oauth2_renders_rather_than_failing_spec_construction():
    """`AuthConfig` rejects oauth2 without a curated provider key.

    The converter's downgrade to a pre-issued bearer token is what keeps this
    from being a render-time crash, so the proof belongs here and not in the
    converter suite.
    """
    generated, spec = _render({"k": {"type": "oauth2", "flows": {}}})
    assert generated.is_valid
    assert spec["auth"]["type"] == "bearer"


def test_a_multi_resource_document_renders_every_resource():
    doc = _doc(paths={
        "/widgets": {"get": {"responses": {"200": {}}}},
        "/orders": {"get": {"responses": {"200": {}}}},
    })
    generated = _import_builder()().build_from_dict(openapi_to_connector_spec(doc).spec)
    assert 'resource == "widgets"' in generated.code or '"widgets"' in generated.code
    assert '"orders"' in generated.code


# --------------------------------------------------------------------------- #
# write_artifacts: the two guards it borrows from save_to_directory
# --------------------------------------------------------------------------- #

class _FakeGenerated:
    def __init__(self, is_valid=True, code="print('hi')\n", errors=()):
        self.is_valid = is_valid
        self.code = code
        self.validation_errors = list(errors)
        self.metadata = "{}"
        self.requirements = "requests\n"
        self.dockerfile = ""


def test_an_invalid_connector_is_never_written(tmp_path):
    """`save_to_directory` refuses these; the CLI does not call it, so it must too."""
    with pytest.raises(OpenAPIConversionError) as excinfo:
        write_artifacts(_FakeGenerated(is_valid=False, errors=["bad thing"]), {}, str(tmp_path / "out"))
    assert "bad thing" in str(excinfo.value)
    assert not (tmp_path / "out").exists()


def test_an_empty_connector_body_is_never_written(tmp_path):
    with pytest.raises(OpenAPIConversionError):
        write_artifacts(_FakeGenerated(code="   \n"), {}, str(tmp_path / "out"))
    assert not (tmp_path / "out").exists()


def test_the_spec_is_written_alongside_the_code(tmp_path):
    """The spec is an editable artifact: change it and re-render, no document needed."""
    out = tmp_path / "out"
    written = write_artifacts(_FakeGenerated(), {"name": "widgets"}, str(out))
    assert sorted(os.path.basename(p) for p in written) == [
        "connector.py", "metadata.json", "requirements.txt", "spec.json",
    ]
    assert json.loads((out / "spec.json").read_text(encoding="utf-8")) == {"name": "widgets"}


# --------------------------------------------------------------------------- #
# Document loading
# --------------------------------------------------------------------------- #

def test_a_yaml_document_is_read_when_it_is_not_json(tmp_path):
    """Most published specs are YAML; JSON is only tried first for its errors."""
    pytest.importorskip("yaml")
    path = tmp_path / "api.yaml"
    path.write_text("openapi: 3.0.3\ninfo:\n  title: Widget Co API\n", encoding="utf-8")
    assert load_document(str(path))["info"]["title"] == "Widget Co API"


def test_a_missing_file_is_reported_by_name(tmp_path):
    with pytest.raises(OpenAPIConversionError) as excinfo:
        load_document(str(tmp_path / "absent.json"))
    assert "absent.json" in str(excinfo.value)


def test_an_empty_file_is_refused(tmp_path):
    path = tmp_path / "api.json"
    path.write_text("", encoding="utf-8")
    with pytest.raises(OpenAPIConversionError):
        load_document(str(path))


# --------------------------------------------------------------------------- #
# The command line, end to end
# --------------------------------------------------------------------------- #

@pytest.fixture
def spec_file(tmp_path):
    path = tmp_path / "widgetco.json"
    path.write_text(
        json.dumps(_doc({"k": {"type": "apiKey", "in": "header", "name": "X-Api-Key"}})),
        encoding="utf-8",
    )
    return path


def test_the_cli_writes_a_complete_connector(tmp_path, spec_file, capsys):
    out = tmp_path / "connector"
    assert run([str(spec_file), "--out", str(out)]) == 0
    assert sorted(p.name for p in out.iterdir()) == [
        "Dockerfile", "connector.py", "metadata.json", "requirements.txt", "spec.json",
    ]
    compile((out / "connector.py").read_text(encoding="utf-8"), "connector.py", "exec")
    assert "widgets" in capsys.readouterr().out


def test_the_cli_dry_run_writes_nothing(tmp_path, spec_file, capsys):
    assert run([str(spec_file)]) == 0
    assert list(tmp_path.iterdir()) == [spec_file]
    assert "Dry run" in capsys.readouterr().out


def test_the_cli_refuses_to_clobber_an_existing_directory(tmp_path, spec_file, capsys):
    out = tmp_path / "connector"
    out.mkdir()
    (out / "connector.py").write_text("# hand-edited\n", encoding="utf-8")

    assert run([str(spec_file), "--out", str(out)]) == 2
    assert (out / "connector.py").read_text(encoding="utf-8") == "# hand-edited\n"
    assert "--force" in capsys.readouterr().err

    assert run([str(spec_file), "--out", str(out), "--force"]) == 0
    assert (out / "connector.py").read_text(encoding="utf-8") != "# hand-edited\n"


def test_the_cli_reports_a_bad_document_without_a_traceback(tmp_path, capsys):
    path = tmp_path / "not-a-spec.json"
    path.write_text('{"info": {"title": "Nope"}}', encoding="utf-8")
    assert run([str(path)]) == 2
    captured = capsys.readouterr()
    assert captured.err.startswith("error: ")
    assert "Traceback" not in captured.err


def test_the_cli_reads_a_document_from_stdin(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr("sys.stdin", __import__("io").StringIO(json.dumps(_doc())))
    assert run(["-"]) == 0
    assert "widgets" in capsys.readouterr().out


def test_cli_overrides_reach_the_written_spec(tmp_path, spec_file):
    out = tmp_path / "connector"
    assert run([
        str(spec_file), "--out", str(out),
        "--name", "widgetco", "--display-name", "WidgetCo",
        "--base-url", "https://self-hosted.example.com",
        "--with-destination",
    ]) == 0
    spec = json.loads((out / "spec.json").read_text(encoding="utf-8"))
    assert spec["name"] == "widgetco"
    assert spec["display_name"] == "WidgetCo"
    assert spec["base_url"] == "https://self-hosted.example.com"
    assert spec["supports_destination"] is True


def _strip_generation_stamp(text: str) -> str:
    """Drop the one line per artifact that carries the wall-clock time.

    `builder.py` stamps `datetime.now().isoformat()` into every artifact it
    renders (`generator/builder.py:392`), so the scaffolder's output is
    reproducible in every respect EXCEPT this line. The exemption is anchored to
    the exact three spellings the templates use so it cannot quietly widen into
    "ignore anything that differs".
    """
    kept = []
    for line in text.splitlines(keepends=True):
        stripped = line.strip()
        if (
            stripped.startswith("Generated: ")
            or stripped.startswith("# Generated: ")
            or stripped.startswith('"generated_at": ')
        ):
            continue
        kept.append(line)
    return "".join(kept)


def test_the_generation_stamp_is_the_only_thing_two_runs_disagree_on(tmp_path, spec_file):
    """Reproducibility is what makes a scaffolded connector reviewable.

    A regenerated connector should diff cleanly against the committed one, so a
    reviewer reads the change and not the noise. Everything the scaffolder
    controls is reproducible; the wall-clock stamp `builder.py` adds is not, and
    this test pins the gap to exactly that.
    """
    first, second = tmp_path / "a", tmp_path / "b"
    assert run([str(spec_file), "--out", str(first)]) == 0
    assert run([str(spec_file), "--out", str(second)]) == 0

    for name in ("connector.py", "spec.json", "requirements.txt", "metadata.json", "Dockerfile"):
        a = (first / name).read_text(encoding="utf-8")
        b = (second / name).read_text(encoding="utf-8")
        assert _strip_generation_stamp(a) == _strip_generation_stamp(b), name

    # The spec carries no stamp at all, so it must match byte for byte. This is
    # also the control: if the helper above were eating whole files, this
    # assertion would still hold while the loop went vacuous, so assert the
    # rendered code is substantial too.
    assert (first / "spec.json").read_text(encoding="utf-8") == (second / "spec.json").read_text(encoding="utf-8")
    assert len(_strip_generation_stamp((first / "connector.py").read_text(encoding="utf-8")).splitlines()) > 200


def test_the_stamp_stripper_removes_one_line_and_not_the_file(tmp_path, spec_file):
    """Without this the exemption could silently grow to cover a real diff."""
    out = tmp_path / "connector"
    assert run([str(spec_file), "--out", str(out)]) == 0
    body = (out / "connector.py").read_text(encoding="utf-8")
    assert len(body.splitlines()) - len(_strip_generation_stamp(body).splitlines()) == 1
