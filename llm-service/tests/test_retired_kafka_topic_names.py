"""llm-service builds none of the Kafka topic names retired before v0.1.6.

These names had builders in this service but no client that created or read
the topics:

- ``rsync.agent.planner.requests`` / ``.responses``: the planner's Kafka loop
  (the deleted planner/kafka_consumer.py). Every deployment ran it disabled.
- ``rsync.conn.<name>`` and ``rsync.protected.conn.<name>``: kafka_message.py
  topic_name() and protected_topic_name(), called only by their own tests.
- ``transformed.<pipeline>.<destination>``: kafka_message.py
  transformed_topic_name(), which had no caller at all.
- ``cdc.<tenant>.<db>.<pipeline>``: the default topic_prefix that
  CDCPipelineConfig.__post_init__ invented. The real CDC prefix is set by the
  debezium connector.

A builder whose topic nothing creates is how names drift from the topics that
exist. A name built again means a producer that auto-creates a stray topic, or
a consumer waiting on one that never appears.

The scan reads string literals, including the literal parts of f-strings, and
skips docstrings. The removal is explained in prose where it happened, and that
prose must not trip this check. A comment is not a string literal, so comments
are never scanned.
"""

import ast
import re
import sys
from pathlib import Path

import pytest

LLM_SERVICE = Path(__file__).resolve().parents[1]
SRC = LLM_SERVICE / "src"
sys.path.insert(0, str(LLM_SERVICE))

# Matched against each literal, with every f-string placeholder rendered as
# "{}". So f"conn.{normalized}" reads as "conn.{}".
RETIRED = {
    "agent.* request/response topic": re.compile(r"\bagent\.[a-z_]+\.(requests|responses)\b"),
    "rsync.conn.<name> / rsync.protected.conn.<name>": re.compile(r"^(protected\.)?conn\.\{\}"),
    "transformed.<pipeline>.<destination>": re.compile(r"^transformed\.\{\}\.\{\}"),
    "cdc.<tenant>.<db>.<pipeline>": re.compile(r"^cdc\.\{\}\.\{\}\.\{\}"),
    "platform topic removed before v0.1.6": re.compile(
        r"^(rsync\.)?(task\.assignments|task\.results|pipeline\.failed\.dlq|healer\.actions"
        r"|sentinel\.audit|pipeline\.agent\.telemetry)$"
    ),
}


def _docstring_nodes(tree: ast.AST) -> set:
    ids = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            body = getattr(node, "body", [])
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
                if isinstance(body[0].value.value, str):
                    ids.add(id(body[0].value))
    return ids


def _literals(tree: ast.AST):
    """Yield (lineno, text) for every string literal outside a docstring."""
    skip = _docstring_nodes(tree)
    inside_fstring = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.JoinedStr):
            parts = []
            for value in node.values:
                inside_fstring.add(id(value))
                if isinstance(value, ast.Constant) and isinstance(value.value, str):
                    parts.append(value.value)
                else:
                    parts.append("{}")
            yield node.lineno, "".join(parts)
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and id(node) not in skip
            and id(node) not in inside_fstring
        ):
            yield node.lineno, node.value


def _offences(source: str, filename: str = "<string>"):
    tree = ast.parse(source, filename=filename)
    found = []
    for lineno, text in _literals(tree):
        for label, pattern in RETIRED.items():
            if pattern.search(text):
                found.append((lineno, label, text))
    return found


def _production_sources():
    for path in sorted(SRC.rglob("*.py")):
        rel = path.relative_to(LLM_SERVICE)
        if ".venv" in rel.parts or "__pycache__" in rel.parts:
            continue
        # Tests may name a retired topic to assert that it is gone.
        if "tests" in rel.parts or path.name.startswith("test_"):
            continue
        yield path


# A positive control for every pattern. Each one is the shape the deleted code
# actually built, so a scanner that stops seeing it fails here, before a clean
# scan of src/ can be read as a pass.
@pytest.mark.parametrize(
    "snippet, label",
    [
        ('REQUEST_TOPIC = topic("agent.planner.requests")\n', "agent.* request/response topic"),
        ('def f(n):\n    return topic(f"conn.{n}")\n', "rsync.conn.<name> / rsync.protected.conn.<name>"),
        ('def f(n):\n    return topic(f"protected.conn.{n}")\n', "rsync.conn.<name> / rsync.protected.conn.<name>"),
        ('def f(p, d):\n    return f"transformed.{p}.{d}"\n', "transformed.<pipeline>.<destination>"),
        (
            'class C:\n    def g(self):\n        self.p = f"cdc.{self.t}.{self.s.database}.{self.i}"\n',
            "cdc.<tenant>.<db>.<pipeline>",
        ),
        ('T = "rsync.task.assignments"\n', "platform topic removed before v0.1.6"),
    ],
)
def test_the_scanner_sees_each_retired_shape(snippet, label):
    assert [o[1] for o in _offences(snippet)] == [label]


def test_the_scanner_ignores_docstrings_comments_and_lookalikes():
    source = (
        '"""Mentions agent.planner.requests in prose."""\n'
        "# topic(f\"conn.{n}\") in a comment\n"
        "def f(conn, sanitized):\n"
        '    """So does this: transformed.<pipeline>.<destination>."""\n'
        '    conn.execute("x")\n'
        '    return ["conn.commit()", f"cdc.{sanitized}"]\n'
    )
    assert _offences(source) == []


def test_no_production_source_builds_a_retired_topic_name():
    files = list(_production_sources())
    # Denominator check. An empty walk scans nothing and passes.
    assert len(files) > 50, f"scanned only {len(files)} files under {SRC}"
    offences = []
    for path in files:
        for lineno, label, text in _offences(path.read_text(encoding="utf-8"), str(path)):
            offences.append(f"{path.relative_to(LLM_SERVICE)}:{lineno}: {label}: {text!r}")
    assert not offences, "retired Kafka topic names are being built again:\n" + "\n".join(offences)


def test_a_cdc_pipeline_config_does_not_invent_a_topic_prefix():
    from src.agents.planner.cdc_pipeline_model import (
        CDCPipelineConfig,
        DestinationConfig,
        DestinationType,
        SourceConfig,
        SourceType,
    )

    config = CDCPipelineConfig(
        pipeline_id="pipeline-001",
        pipeline_name="p",
        tenant_id="tenant-acme",
        created_by="u",
        source=SourceConfig(
            source_type=SourceType.MYSQL,
            connection_id="c",
            host="h",
            port=3306,
            username="u",
            password="x",
            database="production_db",
        ),
        destination=DestinationConfig(destination_type=DestinationType.DATABASE, connection_id="d"),
    )
    assert config.topic_config.topic_prefix == ""
    assert config.to_dict()["topic_config"]["topic_prefix"] == ""
