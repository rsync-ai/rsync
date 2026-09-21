"""The community image must be able to render a connector, not merely contain files.

`test_oss_image_boundary.py` proves the two lists PARTITION the tracked tree: every
file is shipped or stripped, never both, never neither. That is a necessary check and
not a sufficient one. A partition can be perfectly exhaustive and disjoint while the
shipped half cannot import itself — `Dockerfile.community` COPYs a module, that module
imports one on the stripped side, and the boundary test stays green because both files
are correctly classified. The failure surfaces at container start, in the image the
quickstart pulls, with no build-time signal at all.

So this file asks the question the partition cannot: materialise EXACTLY the paths the
allowlist names, put nothing else on `sys.path`, and render a connector end to end.

Ground truth rather than a static approximation, deliberately. An AST walk over the
shipped modules would resolve `import` statements and miss `templates/` entirely —
Jinja loads those from disk by filename (`generator/builder.py` builds a
`FileSystemLoader` over `TEMPLATES_DIR`), so no import statement names them and their
absence is invisible to any import-graph analysis. It is also the single most likely
thing to be forgotten when someone adds a COPY line, because it is the one shipped
directory that contains no Python.

`test_the_simulation_can_fail` is the denominator. Without it, a simulation that
silently stopped exercising the renderer would pass exactly as loudly as a correct one —
the failure mode this repo has hit before. It deletes one required subtree from the
same tree the positive test builds and requires the render to break.

Rendering is necessary and still not sufficient, which is why
`test_every_shipped_module_imports` exists alongside it. The renderer touches the
modules it needs; a shipped module NOTHING imports can be broken in the image forever
without any of the above noticing. That is not hypothetical — it is precisely the state
`schemas/context.py` was in before this boundary moved. It sat on the shipped side while
importing three modules from `agents/`, so in the community image it was an
unconditionally un-importable file, and a mutation restoring it leaves the partition
check and the render check both green.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys

import pytest

import _cut_collection

LLM_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
DOCKERFILE_COMMUNITY = os.path.join(LLM_DIR, "Dockerfile.community")
STRIP_LIST = os.path.join(LLM_DIR, "oss-strip-list.txt")

# The renderer's entrypoint, as a self-hosted user would invoke it.
CLI_MODULE = "agents.tool_generator.scaffold.cli"

# Only the trees the image is an allowlist over. `requirements.txt` and friends are
# COPY'd too but say nothing about whether the Python closes.
_PARTITIONED = ("src", "prompts")

_SPEC = """{
  "openapi": "3.0.0",
  "info": {"title": "Petstore", "version": "1.0", "description": "Pets and owners."},
  "servers": [{"url": "https://petstore.example.com/v1"}],
  "components": {"securitySchemes": {"key": {"type": "apiKey", "in": "header", "name": "X-Api-Key"}}},
  "security": [{"key": []}],
  "paths": {
    "/pets": {
      "get": {"operationId": "listPets", "responses": {"200": {"description": "ok"}}},
      "post": {"operationId": "createPet", "responses": {"201": {"description": "created"}}}
    },
    "/pets/{petId}": {
      "get": {"operationId": "getPet", "responses": {"200": {"description": "ok"}}},
      "delete": {"operationId": "deletePet", "responses": {"204": {"description": "gone"}}}
    }
  }
}
"""


def _copy_sources():
    """Source paths named by COPY lines — the same parse the boundary guard uses.

    Kept as a local copy rather than imported from `test_oss_image_boundary` so that
    a regex loosened for one file's benefit cannot quietly change the other's subject.
    """
    srcs = []
    with open(DOCKERFILE_COMMUNITY, encoding="utf-8") as fh:
        for line in fh:
            m = re.match(r"^\s*COPY\s+(?!--)(\S+)\s+(\S+)\s*$", line)
            if m:
                srcs.append(m.group(1).rstrip("/"))
    return [s for s in srcs if s.split("/")[0] in _PARTITIONED]


def _strip_list():
    out = []
    with open(STRIP_LIST, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line and not line.startswith("#"):
                out.append(line.rstrip("/"))
    return out


def _materialise(dest):
    """Build a tree holding exactly what Dockerfile.community COPYs from src/ + prompts/."""
    copied = 0
    for rel in _copy_sources():
        src = os.path.join(LLM_DIR, rel)
        dst = os.path.join(dest, rel)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        if os.path.isdir(src):
            shutil.copytree(
                src, dst, dirs_exist_ok=True,
                ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
            )
        else:
            shutil.copy2(src, dst)
        copied += 1
    return copied


def _render(tree, out_dir, spec_path):
    """Run the scaffolder with `tree/src` as the ONLY source of first-party modules.

    `env -i`-equivalent: no inherited PYTHONPATH could put the stripped tree back on
    the path and turn a real failure green, and no inherited credentials reach a
    process whose whole point is that it needs none.
    """
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": tree,
        "PYTHONPATH": os.path.join(tree, "src"),
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    return subprocess.run(
        [sys.executable, "-m", CLI_MODULE, spec_path, "-o", out_dir],
        cwd=os.path.join(tree, "src"),
        env=env, capture_output=True, text=True,
    )


@pytest.fixture(scope="module")
def spec_file(tmp_path_factory):
    p = tmp_path_factory.mktemp("spec") / "petstore.json"
    p.write_text(_SPEC, encoding="utf-8")
    return str(p)


@pytest.fixture(scope="module")
def image_tree(tmp_path_factory):
    tree = str(tmp_path_factory.mktemp("community-image"))
    copied = _materialise(tree)
    # Vacuity guard: a COPY parse that matched nothing would give every assertion
    # below an empty tree to be trivially wrong about.
    assert copied >= 10, f"Dockerfile.community COPY parse yielded {copied} src/prompts paths"
    return tree


def test_the_image_tree_holds_no_stripped_path(image_tree):
    """The allowlist is only a promise until something checks what landed."""
    leaked = []
    for entry in _strip_list():
        if os.path.exists(os.path.join(image_tree, entry)):
            leaked.append(entry)
    assert not leaked, (
        "these moat paths are present in a tree built from Dockerfile.community's "
        f"allowlist, so the published image would carry them: {leaked}"
    )


def test_the_community_image_renders_a_connector(image_tree, tmp_path, spec_file):
    """The whole point of Stage 2: a self-hosted user can scaffold from a spec."""
    out = str(tmp_path / "out")
    proc = _render(image_tree, out, spec_file)
    assert proc.returncode == 0, (
        "the scaffolder cannot run inside a tree containing only what the community "
        "image ships. Something it needs is on the stripped side of the boundary.\n"
        f"--- stdout ---\n{proc.stdout}\n--- stderr ---\n{proc.stderr}"
    )

    connector = os.path.join(out, "connector.py")
    assert os.path.exists(connector), f"no connector.py in {sorted(os.listdir(out))}"
    body = open(connector, encoding="utf-8").read()
    # Substantive, not a stub the renderer emitted on its way to failing quietly.
    assert len(body.splitlines()) > 200, f"connector.py is {len(body.splitlines())} lines"
    assert "X-Api-Key" in body, "the declared api_key header did not reach the render"


_IMPORT_WALK = r"""
import importlib, json, os, sys
root = sys.argv[1]
sys.path.insert(0, os.path.join(root, "src"))
pkg_root = os.path.join(root, "src", "agents", "tool_generator")
fails = []
for dirpath, dirnames, filenames in os.walk(pkg_root):
    dirnames[:] = [d for d in dirnames if d != "__pycache__"]
    for fn in sorted(filenames):
        if not fn.endswith(".py"):
            continue
        rel = os.path.relpath(os.path.join(dirpath, fn), os.path.join(root, "src"))
        mod = rel[:-3].replace(os.sep, ".")
        if mod.endswith(".__init__"):
            mod = mod[: -len(".__init__")]
        try:
            importlib.import_module(mod)
        except ModuleNotFoundError as exc:
            fails.append({"module": mod, "missing": exc.name or "", "kind": "missing"})
        except BaseException as exc:
            fails.append({"module": mod, "missing": "", "kind": type(exc).__name__,
                          "detail": str(exc).splitlines()[0][:200]})
print(json.dumps(fails))
"""


def test_every_shipped_module_imports(image_tree, tmp_path):
    """A shipped module that cannot import is broken in the image, imported or not.

    The renderer only exercises its own closure, so this walks the whole shipped
    generation package instead and imports each module on its own.

    Third-party gaps are not boundary defects and must not be reported as ones: this
    suite runs in whatever environment CI gives it, while the real image installs
    `requirements.txt`. The two are told apart by asking whether the missing module is
    one of OURS — `agents.tool_generator.agents` is, and the image not having it is
    the boundary cutting through an import; `fastapi` is not, and this environment
    simply lacks the dependency. `_cut_collection.is_first_party` answers that from
    the source tree AND the strip list, not from the tree alone, because the tree
    alone stops being able to answer once the flip has deleted the stripped half —
    see its docstring. Nothing about it is a hand-maintained package allowlist, so it
    cannot rot into exempting a real failure.

    Non-import errors are reported too. A module raising at import time is just as
    broken in the image, and swallowing those would leave the check able to pass on a
    tree where every module explodes for some other reason.
    """
    proc = subprocess.run(
        [sys.executable, "-c", _IMPORT_WALK, image_tree],
        cwd=os.path.join(image_tree, "src"),
        env={
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "HOME": image_tree,
            "PYTHONDONTWRITEBYTECODE": "1",
        },
        capture_output=True, text=True,
    )
    assert proc.returncode == 0, f"the import walk itself failed:\n{proc.stderr}"

    payload = [ln for ln in proc.stdout.splitlines() if ln.startswith("[")]
    assert payload, f"import walk produced no result line:\n{proc.stdout}\n{proc.stderr}"
    failures = __import__("json").loads(payload[-1])

    first_party, other = [], []
    for f in failures:
        if f["kind"] != "missing":
            other.append(f"{f['module']}: {f['kind']}: {f.get('detail', '')}")
            continue
        # Is the missing module one of ours? Then its absence is the boundary, not
        # the environment.
        if _cut_collection.is_first_party(f["missing"]):
            first_party.append(f"{f['module']} -> {f['missing']}")
        else:
            other.append(f"{f['module']} -> {f['missing']} (third-party, not a boundary defect)")

    assert not first_party, (
        "these modules ship in the community image but import something the image "
        "strips, so they are un-importable there. Either move the module to the "
        "stripped side or promote what it needs:\n  " + "\n  ".join(first_party)
    )


def test_the_import_walk_can_fail(image_tree, tmp_path):
    """Control for the check above, against the exact defect that motivated it.

    Plants a module on the shipped side importing the stripped `agents/` package —
    what `schemas/context.py` did — and requires it to be classified first-party. If
    this stops failing, the discriminator has been widened into an exemption.

    It runs on a cut tree too, and has to: the discriminator's whole job is telling a
    boundary defect from a missing dependency, and the public repo is where such a
    defect actually reaches a user. An earlier draft asked the filesystem directly
    here and passed privately while failing on the cut tree — with the failure text
    "this repo has no path for it", which is the answer the primary check would have
    silently accepted as third-party.
    """
    tree = str(tmp_path / "planted")
    shutil.copytree(image_tree, tree)
    planted = os.path.join(tree, "src", "agents", "tool_generator", "schemas", "_control.py")
    with open(planted, "w", encoding="utf-8") as fh:
        fh.write("from ..agents.base import AgentResult  # noqa: F401\n")

    proc = subprocess.run(
        [sys.executable, "-c", _IMPORT_WALK, tree],
        cwd=os.path.join(tree, "src"),
        env={
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "HOME": tree,
            "PYTHONDONTWRITEBYTECODE": "1",
        },
        capture_output=True, text=True,
    )
    assert proc.returncode == 0, proc.stderr
    failures = __import__("json").loads(
        [ln for ln in proc.stdout.splitlines() if ln.startswith("[")][-1]
    )
    hits = [f for f in failures if f["module"].endswith("schemas._control")]
    assert hits, (
        "a module importing the stripped agents/ package imported cleanly in a tree "
        "built from the community allowlist — the walk is not seeing what it claims to"
    )
    missing = hits[0]["missing"]
    assert _cut_collection.is_first_party(missing), (
        f"the control failed on {missing!r}, which the discriminator does not read as "
        "one of ours, so it would file this real defect under 'third-party'"
    )


@pytest.mark.parametrize(
    "subtree",
    ["contracts", "generator", "schemas", "templates", "validation"],
)
def test_the_simulation_can_fail(image_tree, tmp_path, spec_file, subtree):
    """Delete one required subtree and the render must break.

    Every entry here is a directory `Dockerfile.community` COPYs. If one stops being
    required, that is a real change in what the image needs and the parametrisation
    should shrink deliberately — not be discovered when the positive test above has
    quietly stopped proving anything.
    """
    tree = str(tmp_path / f"without-{subtree}")
    shutil.copytree(image_tree, tree)
    victim = os.path.join(tree, "src", "agents", "tool_generator", subtree)
    assert os.path.isdir(victim), f"{subtree}/ is not in the image tree to begin with"
    shutil.rmtree(victim)

    proc = _render(tree, str(tmp_path / f"out-{subtree}"), spec_file)
    assert proc.returncode != 0, (
        f"the render SUCCEEDED with {subtree}/ deleted, so this simulation is not "
        "actually exercising it and the positive test above proves less than it "
        f"appears to.\n--- stdout ---\n{proc.stdout}"
    )
