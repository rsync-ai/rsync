"""No connector's metadata may claim CDC that the CDC policy denies it.

Databricks shipped `supports_cdc: true` in a `data_warehouse` connector. There is
no Databricks CDC: no provider is registered in `backend-orchestrator/internal/cdc`
(`RegisterProvider` is called by postgresql, mysql, sqlserver, oracle, mongodb only),
`capability_rules.yaml` gives `data_warehouse` a `supports_cdc: false` default, and
the generator's own inferrer force-overrides an LLM that proposes otherwise.

The list endpoint hid it. `tools.go:1576` recomputes `SupportsCDC` from
`isCDCExposed()` before serving, so the catalogue showed the truth while the file
on disk said the opposite -- which is exactly the shape that survives review. The
readers that do NOT recompute got the lie: `connectorSupportsCDC`
(`api-gateway/internal/handlers/chat_nl_pipeline.go:46`) returns the raw metadata
field and routes a natural-language pipeline request on it.

What this suite asserts, and why each part is here rather than folded into one test:

* Metadata agrees with policy, in BOTH directions -- a connector that quietly
  stops claiming CDC it really has is the same defect mirrored.
* The nested `capabilities.supports_cdc` agrees with the top-level field. The
  Databricks file carried the wrong value twice; fixing one would have left a
  second copy for the next reader to find.
* The Go allowlist and the YAML allowlist are equal. Everything above measures
  metadata against the YAML, so if the two policies drift, this suite is grading
  against a policy the product does not run. `tools.go:586` asks in a comment for
  them to be kept in sync; a comment is not a check.
* The identifier is `id`, never the display name. `SQL Server` normalises to
  `sql_server`, which is not in the allowlist -- keying on `name` reports a false
  mismatch for the six connectors whose id and name differ.

Subject: connector metadata (`shared/mcp-connectors/**`) and `tools.go`. Both are
listed in ci.yml's `llm` paths-filter, so editing either runs this file.
"""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "generate_connector_reference.py"
TOOLS_GO = REPO / "api-gateway" / "internal" / "handlers" / "tools.go"

# The three internal components under shared/mcp-connectors/internal. They are
# appended to the connector list only under `includeInternal` (tools.go:1463), an
# admin/power_user branch that never calls isCDCExposed() -- so the policy this
# suite enforces does not apply to them, and `supports_cdc: true` on the Debezium
# engine and the Kafka CDC sink worker is a true statement about a CDC component,
# not a false claim about a selectable source. Named, not pattern-matched, so a
# NEW internal connector fails the exemption test and has to be reasoned about.
INTERNAL_EXEMPT = {"debezium", "kafka-mcp-sink", "minio"}


@pytest.fixture(scope="module")
def gen():
    """The reference generator, reused for its discovery and its policy mirror."""
    spec = importlib.util.spec_from_file_location("connector_reference_gen", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def public(gen):
    """[(id, metadata)] for every tracked public connector."""
    out = []
    for d in gen.tracked_connector_dirs("public"):
        md = gen.read_metadata(d)
        if md is not None:
            out.append((d.name, md))
    return out


@pytest.fixture(scope="module")
def internal(gen):
    out = []
    for d in gen.tracked_connector_dirs("internal"):
        md = gen.read_metadata(d)
        if md is not None:
            out.append((d.name, md))
    return out


def test_the_sweep_actually_sees_connectors(public, internal):
    """A count of zero is a pass for every assertion below. Floor it explicitly."""
    assert len(public) >= 20, f"only {len(public)} public connectors discovered"
    assert len(internal) >= 3, f"only {len(internal)} internal connectors discovered"


def test_metadata_supports_cdc_matches_policy(public, gen):
    allowlist = gen.load_cdc_allowlist()
    wrong = [
        (cid, bool(md.get("supports_cdc")), gen.cdc_exposed(md, allowlist), md.get("category"))
        for cid, md in public
    ]
    wrong = [w for w in wrong if w[1] != w[2]]
    assert not wrong, "metadata disagrees with the CDC policy:\n" + "\n".join(
        f"  {cid}: metadata says supports_cdc={meta}, policy says {policy} (category={cat})"
        for cid, meta, policy, cat in wrong)


def test_nested_capabilities_agree_with_top_level(public):
    """Databricks carried the wrong value in both places; one fix is half a fix."""
    disagree = [
        (cid, bool(md.get("supports_cdc")), bool(md["capabilities"]["supports_cdc"]))
        for cid, md in public
        if isinstance(md.get("capabilities"), dict)
        and "supports_cdc" in md["capabilities"]
    ]
    disagree = [d for d in disagree if d[1] != d[2]]
    assert not disagree, "top-level and capabilities.supports_cdc disagree:\n" + "\n".join(
        f"  {cid}: top-level={top}, capabilities={nested}" for cid, top, nested in disagree)


def test_policy_can_fail(gen):
    """Control. Without this, a `cdc_exposed` stuck at False passes every test above.

    Two synthetic connectors, one on each side of the gate, so the assertion is
    that the policy DISCRIMINATES -- not merely that it returns something.
    """
    allowlist = gen.load_cdc_allowlist()
    assert gen.cdc_exposed({"id": "postgresql", "category": "relational_db"}, allowlist) is True
    assert gen.cdc_exposed({"id": "postgresql", "category": "data_warehouse"}, allowlist) is False
    assert gen.cdc_exposed({"id": "databricks", "category": "relational_db"}, allowlist) is False


def test_internal_components_are_the_only_exemptions(internal):
    """The exemption is `internal: true`, and it must stay a closed, small set."""
    assert {cid for cid, _ in internal} == INTERNAL_EXEMPT, (
        "the set of internal connectors changed; a new one is exempt from the CDC "
        "policy by virtue of living here, so decide deliberately whether its "
        "supports_cdc claim is true and then update INTERNAL_EXEMPT")
    not_marked = [cid for cid, md in internal if not md.get("internal")]
    assert not not_marked, (
        f"{not_marked} live under internal/ but do not set `internal: true`, so "
        "tools.go would list them to ordinary users without the admin gate")


def test_the_go_allowlist_parser_works_on_this_tree(gen):
    """The fallback path, exercised on BOTH trees -- including the one that uses it.

    `capability_rules.yaml` is stripped by llm-service/oss-strip-list.txt while
    `scripts/generate_connector_reference.py` ships and is advertised at
    docs/connectors/reference.md, so on the public tree the Go map is the ONLY copy
    of this policy and `load_cdc_allowlist()` parses it. A fallback that only runs
    where it is never needed is not a fallback.
    """
    names = gen._allowlist_from_go()
    assert "postgresql" in names and "mysql" in names, sorted(names)
    assert len(names) >= 5, f"parsed only {sorted(names)} -- the parser is stale"
    assert names == gen.load_cdc_allowlist() or gen.CAPABILITY_RULES.is_file(), (
        "load_cdc_allowlist() disagrees with the Go map on a tree that has no "
        f"{gen.CAPABILITY_RULES.name} to prefer: {sorted(gen.load_cdc_allowlist())}"
    )


def test_go_and_yaml_cdc_allowlists_are_in_lockstep(gen):
    """tools.go:586 asks for this in a comment. This is the part that enforces it."""
    if not gen.CAPABILITY_RULES.is_file():
        # Only one copy exists here, and load_cdc_allowlist() reads it out of
        # tools.go -- so this comparison would compare tools.go with itself and
        # pass having checked nothing. Lockstep between two files is an invariant
        # of the tree that HAS two files. The parser itself is covered above.
        pytest.skip(
            f"{gen.CAPABILITY_RULES.name} is absent (llm-service/oss-strip-list.txt); "
            "tools.go is the only copy of the CDC allowlist in this tree"
        )
    src = TOOLS_GO.read_text()
    block = re.search(r"var debeziumSupportedDatabases = map\[string\]bool\{(.*?)\n\}",
                      src, re.S)
    assert block, "debeziumSupportedDatabases literal not found in tools.go"
    go_names = set(re.findall(r'"([^"]+)":\s*true', block.group(1)))
    assert len(go_names) >= 5, f"parsed only {sorted(go_names)} from tools.go -- parser is stale"
    assert go_names == gen.load_cdc_allowlist(), (
        f"CDC allowlists have drifted.\n  tools.go only: {sorted(go_names - gen.load_cdc_allowlist())}"
        f"\n  capability_rules.yaml only: {sorted(gen.load_cdc_allowlist() - go_names)}")


def test_go_and_python_cdc_category_gates_agree(gen):
    """isCDCExposed rejects on category before consulting the allowlist."""
    src = TOOLS_GO.read_text()
    body = re.search(r"func isCDCExposed\(.*?\n\}", src, re.S)
    assert body, "isCDCExposed not found in tools.go"
    go_cats = set(re.findall(r'cat != "([a-z_]+)"', body.group(0)))
    assert go_cats == gen.CDC_CATEGORIES, (
        f"category gate drifted: tools.go={sorted(go_cats)}, "
        f"generator={sorted(gen.CDC_CATEGORIES)}")


def test_the_identifier_is_id_not_display_name(public, gen):
    """Locks in why this suite keys on `id`, using the tree as the witness.

    `tools.go:1576` passes `connector.Name`, which `mapToMCPConnector` sets to the
    canonical ID (`tools.go:1432`) -- not to metadata's `name`, which is the human
    label. Keying on `name` silently mis-grades every connector where they differ.
    """
    divergent = [(md.get("id"), md.get("name")) for _, md in public
                 if md.get("id") and md.get("name")
                 and str(md["id"]).lower() != str(md["name"]).lower()]
    assert len(divergent) >= 5, (
        "expected several connectors whose id and display name differ; if this "
        "dropped to zero the test below no longer proves anything")

    allowlist = gen.load_cdc_allowlist()
    sqlserver = next((md for cid, md in public if md.get("id") == "sqlserver"), None)
    assert sqlserver is not None, "sqlserver connector not found"
    assert gen.cdc_exposed(sqlserver, allowlist) is True
    # The same connector, graded on its display name ("SQL Server" -> sql_server),
    # is denied CDC. That is the false mismatch this key choice avoids.
    by_name = dict(sqlserver)
    by_name["id"] = sqlserver["name"]
    assert gen.cdc_exposed(by_name, allowlist) is False
