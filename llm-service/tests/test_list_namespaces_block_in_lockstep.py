"""The list_namespaces block is one piece of text kept in five files.

The PostgreSQL, MySQL, Oracle and SQL Server connectors are hand-curated copies
of connector_database.py.j2, and each carries the same block between the
"BEGIN list_namespaces block" and "END list_namespaces block" markers: the
system-namespace lists, the helpers discover_schema uses to find schemas, and
list_namespaces itself. A fix made to one copy alone would let that connector's
schema picker and its discovery disagree with the others (and a connector
generated from the template would miss the fix). This test fails until every
copy matches the template's.
"""

from __future__ import annotations

import pathlib

import pytest

REPO = pathlib.Path(__file__).resolve().parents[2]
TEMPLATE = REPO / "llm-service" / "src" / "agents" / "tool_generator" / "templates" / "connector_database.py.j2"
_PUBLIC = REPO / "shared" / "mcp-connectors" / "public"
COPIES = {
    "postgresql": _PUBLIC / "postgresql" / "versions" / "v1.0.0" / "connector.py",
    "mysql": _PUBLIC / "database" / "mysql" / "versions" / "v1.0.0" / "connector.py",
    "oracle": _PUBLIC / "database" / "oracle" / "versions" / "v1.0.0" / "connector.py",
    "sqlserver": _PUBLIC / "database" / "sqlserver" / "versions" / "v1.0.0" / "connector.py",
}
BEGIN = "# BEGIN list_namespaces block"
END = "# END list_namespaces block"


def _block(path: pathlib.Path) -> str:
    text = path.read_text(encoding="utf-8")
    assert text.count(BEGIN) == 1 and text.count(END) == 1, (
        f"{path}: want exactly one BEGIN and one END marker, "
        f"found {text.count(BEGIN)} and {text.count(END)}"
    )
    start = text.index(BEGIN)
    return text[start:text.index(END, start) + len(END)]


def test_the_template_block_holds_the_method_and_its_helpers():
    block = _block(TEMPLATE)
    for name in ("def list_namespaces(", "def _postgres_user_schemas(",
                 "def _sqlserver_user_schemas(", "def _oracle_user_owners(",
                 "def _mysql_user_databases("):
        assert name in block, f"{name} is missing from the template's block"
    # Jinja would rewrite these; the block must reach generated code verbatim.
    assert "{{" not in block and "{%" not in block


@pytest.mark.parametrize("name", sorted(COPIES))
def test_each_connector_carries_the_templates_block(name):
    assert _block(COPIES[name]) == _block(TEMPLATE), (
        f"the list_namespaces block in {COPIES[name]} differs from the one in "
        f"{TEMPLATE}; change them together"
    )
