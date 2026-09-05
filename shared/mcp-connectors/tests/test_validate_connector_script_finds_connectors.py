"""``scripts/mcp-connectors/validate_connector.py`` must actually find the connectors.

The script is the contributor-facing gate the developer guide points at, and it was
looking in the wrong tree entirely: ``CONNECTORS_DIR = Path(__file__).parent`` resolved
to ``scripts/mcp-connectors/``, which holds the script and nothing else. Both documented
invocations -- ``--all`` and ``<connector_name>`` -- therefore examined zero connectors.
``--all`` printed "No connectors found"; the named form printed "Connector directory not
found". Neither is in CI, so nothing failed for as long as that was true.

Two further defects were only reachable once discovery worked, and both are the shape
where a check that cannot fire looks exactly like a check that passed:

* the connector class was matched by the name suffix ``MCPServer``, which the twelve
  ``<Name>Connector`` REST/GraphQL connectors do not use. The class came back ``None``
  and every attribute, inheritance, method and response-format check downstream returned
  early -- silently, on 12 of 28 connectors;
* the four core operations were looked up in metadata's ``capabilities``, which is an
  object of feature flags (``supports_cdc``, ``max_batch_size``) in all 28 connectors and
  has never held a tool name. Tool names live under ``operations`` (27) or ``tools`` (1),
  so those four warnings fired on every connector in the repo, always, and meant nothing.

This suite pins the denominator, not the verdict: what matters is that the script looks
at every connector on disk. It deliberately does NOT assert that all of them pass -- the
warnings are the script's product, and asserting them here would just duplicate the
conformance suite next door.
"""
from __future__ import annotations

import ast
import importlib.util
import json
import os
import re
import subprocess
import sys

import pytest

# This file lives in shared/mcp-connectors/tests/, so the connector tree is its parent
# and the repo root is two levels above that.
CONNECTOR_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
REPO_ROOT = os.path.abspath(os.path.join(CONNECTOR_ROOT, "..", ".."))
SCRIPT = os.path.join(REPO_ROOT, "scripts", "mcp-connectors", "validate_connector.py")

# Same floor as test_tool_surface_conformance.py: an empty set reads as a pass, so the
# denominator is asserted before anything is concluded from it. Kept below the real
# count so the four untracked local scratch connectors may be absent in CI.
MIN_CONNECTORS_DISCOVERED = 20


def _connector_roots_on_disk() -> dict:
    """Every connector root, found WITHOUT using the script's own helpers.

    Walks for the shipped artifact (``versions/<v>/connector.py``) and climbs back to
    the root, then reads ``latest.json`` for the version that is actually current. A
    root with two version directories is why "the highest-numbered one" is the wrong
    rule and why this cannot just glob.
    """
    roots = {}
    for dirpath, dirnames, filenames in os.walk(CONNECTOR_ROOT):
        if "connector.py" not in filenames:
            continue
        version_dir = dirpath
        versions_dir = os.path.dirname(version_dir)
        if os.path.basename(versions_dir) != "versions":
            continue
        root = os.path.dirname(versions_dir)
        latest = os.path.join(root, "latest.json")
        if not os.path.isfile(latest):
            continue
        with open(latest) as fh:
            current = (json.load(fh).get("current_version") or "").strip()
        if os.path.basename(version_dir) != current:
            continue  # a non-current version directory, correctly not the canonical one
        roots[os.path.basename(root)] = version_dir
    return roots


def script_standalone_set() -> set:
    """Read NON_SUBCLASS_CONNECTORS out of the script by AST, without importing it."""
    with open(SCRIPT) as fh:
        tree = ast.parse(fh.read())
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "NON_SUBCLASS_CONNECTORS" for t in node.targets
        ):
            return set(ast.literal_eval(node.value))
    return set()


@pytest.fixture(scope="module")
def script_module():
    spec = importlib.util.spec_from_file_location("validate_connector_under_test", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_script_exists_where_the_docs_point():
    assert os.path.isfile(SCRIPT), (
        f"{SCRIPT} is missing. shared/mcp-connectors/README.md and "
        "docs/connectors/developer-guide.md tell contributors to run it."
    )


def test_there_are_connectors_to_find():
    """Anti-vacuity: if this fails, every other case below is meaningless."""
    on_disk = _connector_roots_on_disk()
    assert len(on_disk) >= MIN_CONNECTORS_DISCOVERED, (
        f"only {len(on_disk)} connector roots found under {CONNECTOR_ROOT} "
        f"(floor {MIN_CONNECTORS_DISCOVERED}) -- the walk in this test is broken, "
        "so nothing it concludes about the script means anything"
    )


def test_the_script_resolves_the_same_connectors_the_tree_holds(script_module):
    on_disk = _connector_roots_on_disk()
    found = {
        root.name: str(script_module._resolve_current_dir(root))
        for root in script_module._iter_connector_roots(script_module.CONNECTORS_ROOT)
    }
    found = {name: path for name, path in found.items()
             if os.path.isfile(os.path.join(path, "connector.py"))}

    missing = sorted(set(on_disk) - set(found))
    extra = sorted(set(found) - set(on_disk))
    assert not missing, f"the script does not see these connectors: {missing}"
    assert not extra, f"the script sees connectors that are not on disk: {extra}"

    # And it must resolve each one to the CURRENT version, not merely to some version.
    wrong = {n: (found[n], on_disk[n]) for n in on_disk
             if os.path.realpath(found[n]) != os.path.realpath(on_disk[n])}
    assert not wrong, f"resolved to a non-current version directory: {wrong}"


def test_running_all_examines_every_connector():
    """End-to-end, through the documented command. This is the case that was red."""
    on_disk = _connector_roots_on_disk()
    proc = subprocess.run(
        [sys.executable, SCRIPT, "--all"],
        cwd=REPO_ROOT, capture_output=True, text=True,
    )
    out = proc.stdout + proc.stderr
    match = re.search(r"Found (\d+) connector\(s\) to validate", out)
    assert match, (
        "the script printed no discovery count. Full output:\n"
        + out[-4000:]
    )
    count = int(match.group(1))
    assert count == len(on_disk), (
        f"the script examined {count} connectors; {len(on_disk)} are on disk: "
        f"{sorted(on_disk)}"
    )
    assert count >= MIN_CONNECTORS_DISCOVERED

    # Every connector must appear in the per-connector summary, not just the count.
    for name in on_disk:
        assert re.search(rf"(PASS|FAIL) - {re.escape(name)}$", out, re.M), (
            f"{name} was counted but never reported on"
        )

    # Counted and reported is still not checked. The class-detection defect left
    # self.connector_class None on 12 connectors, and every downstream check returns
    # early on that -- so those 12 were counted, reported, and examined by nothing.
    # Assert the class check actually fired for each non-standalone connector.
    sections = {}
    current = None
    for line in out.splitlines():
        marker = re.match(r"\s*🔍 Validating connector: (\S+)", line)
        if marker:
            current = marker.group(1)
            sections[current] = []
        elif current:
            sections[current].append(line)
    silent = sorted(
        name for name in on_disk
        if name not in script_standalone_set()
        and not any("Connector class found:" in ln for ln in sections.get(name, []))
    )
    assert not silent, (
        "the connector class was never resolved for "
        f"{silent} -- every attribute, inheritance, method and response-format check "
        "returns early when it is None, so those connectors were counted but not checked"
    )

    # Same shape one field over: the core-operation lookup used to read metadata's
    # `capabilities` (feature flags), so it could never succeed for anyone. At least
    # one connector must be able to satisfy it, or the warning is noise again.
    assert "Operation declared: test_connection" in out, (
        "no connector satisfied the core-operation check -- it is reading a field that "
        "never holds tool names, so its warnings mean nothing"
    )


def test_running_a_named_connector_finds_it():
    """The other documented invocation. `postgresql` is not under public/database/ --
    the script must search the tree, not join a fixed prefix."""
    proc = subprocess.run(
        [sys.executable, SCRIPT, "postgresql"],
        cwd=REPO_ROOT, capture_output=True, text=True,
    )
    out = proc.stdout + proc.stderr
    assert "Connector directory not found" not in out, out[-2000:]
    assert "Validating connector: postgresql" in out, out[-2000:]
    assert "Connector class found: PostgresqlMCPServer" in out, out[-2000:]


def test_the_class_rule_covers_both_naming_conventions(script_module):
    """A suffix-only rule missed 12 connectors. Assert the tree really does use two
    conventions, so this stays a live constraint rather than a historical note."""
    on_disk = _connector_roots_on_disk()
    suffixes = {"MCPServer": [], "Connector": [], "other": []}
    for name, version_dir in sorted(on_disk.items()):
        with open(os.path.join(version_dir, "connector.py")) as fh:
            tree = ast.parse(fh.read())
        classes = [n.name for n in tree.body if isinstance(n, ast.ClassDef)]
        picked = [c for c in classes if c.endswith("MCPServer")] or \
                 [c for c in classes if c.endswith("Connector")]
        if not picked:
            suffixes["other"].append(name)
        elif picked[0].endswith("MCPServer"):
            suffixes["MCPServer"].append(name)
        else:
            suffixes["Connector"].append(name)
    assert suffixes["MCPServer"], "no <Name>MCPServer connectors found -- fixture broken"
    assert suffixes["Connector"], (
        "no <Name>Connector connectors found. If the tree really has converged on one "
        "convention, simplify the script's rule; until then this asserts why it needs two."
    )


def test_standalone_set_matches_the_conformance_suite(script_module):
    """The script and the conformance suite each carry the exemption set. They are two
    files, so they can drift; this is the pin that stops it."""
    from test_tool_surface_conformance import NON_SUBCLASS_CONNECTORS as canonical

    assert script_module.NON_SUBCLASS_CONNECTORS == canonical, (
        "scripts/mcp-connectors/validate_connector.py and "
        "shared/mcp-connectors/tests/test_tool_surface_conformance.py disagree about "
        "which connectors are standalone MCP servers: "
        f"{script_module.NON_SUBCLASS_CONNECTORS} vs {canonical}"
    )


def test_ci_runs_this_guard_when_the_script_changes():
    """A guard whose own subject is outside the job's paths filter never runs on the
    change it exists to catch. This suite is in the `llm` lane; the script it guards is
    under scripts/, which that lane did not list."""
    ci = os.path.join(REPO_ROOT, ".github", "workflows", "ci.yml")
    with open(ci) as fh:
        text = fh.read()
    assert "scripts/mcp-connectors/**" in text, (
        "ci.yml's `llm` paths filter does not cover scripts/mcp-connectors/, so a PR "
        "editing validate_connector.py alone would skip this suite entirely."
    )
