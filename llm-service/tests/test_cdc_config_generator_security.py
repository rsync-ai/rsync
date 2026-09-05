"""Kafka security + publication safety on BOTH CDC-config generator paths.

``src/agents/planner/cdc_config_generator.py`` can build a Debezium config two
ways — from a connector's ``metadata.json`` template, or from the built-in
per-database template — and ``generate_config()`` tries the metadata one FIRST.
Anything that must be true of a generated config therefore has to be asserted on
both, which is what this file does.

Why it is worth the ceremony: Debezium's schema history is a SEPARATE Kafka
client inside the connector task. Its producer half runs during snapshot; its
consumer half runs only on task RESTART. A history client with no credentials on
a secured cluster snapshots and streams perfectly for weeks, then fails to resume
on the first restart — reading as data loss or a mystery CDC stall long after the
deploy that caused it.

Prior art for the same invariant one layer down (inside the Debezium MCP
connector): shared/mcp-connectors/internal/debezium/versions/v1.0.0/
test_schema_history_security.py.
"""

import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.agents.planner.cdc_config_generator import CDCConfigGenerator  # noqa: E402

PG_CONNECTOR_CLASS = "io.debezium.connector.postgresql.PostgresConnector"

# The keys the security block owns, on both halves of the history client.
SECURITY_PREFIXES = (
    "schema.history.internal.producer.",
    "schema.history.internal.consumer.",
)

CONNECTION_CONFIG = {
    "host": "db.example.com",
    "port": 5432,
    "username": "svc",
    "password": "pw",
    "database": "app",
}


@pytest.fixture(autouse=True)
def _clear_kafka_env(monkeypatch):
    """Every test starts from an unconfigured environment."""
    for key in list(os.environ):
        if key.startswith("KAFKA_"):
            monkeypatch.delenv(key, raising=False)


@pytest.fixture
def sasl_env(monkeypatch):
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "rsync")
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "s3cret")


def _metadata_dir(tmp_path, source="postgresql", connector_class=PG_CONNECTOR_CLASS, defaults=None):
    """A connectors directory holding one CDC-capable metadata.json.

    This is the shape that makes generate_config() take the metadata path:
    ``supports_cdc`` true AND a ``cdc_config_template``.

    The layout has to be the canonical one -- ``latest.json`` at the connector
    root pointing at ``versions/<current_version>/``, which is where the real
    metadata.json lives. This fixture used to write ``<root>/<source>/metadata.json``
    instead, the root-copy layout that was deleted from the repo, and that is
    exactly how the loader defect stayed hidden: these tests exercised the
    metadata path successfully against a directory shape no deployment has had
    since. Coverage of a fiction reads identically to coverage.
    """
    root = tmp_path / "connectors"
    conn = root / source
    version = "v1.0.0"
    versioned = conn / "versions" / version
    versioned.mkdir(parents=True)
    (conn / "latest.json").write_text(
        json.dumps({"current_version": version, "all_versions": [version]})
    )
    (versioned / "metadata.json").write_text(
        json.dumps(
            {
                "name": source,
                "supports_cdc": True,
                "cdc_config_template": {
                    "connector_class": connector_class,
                    "tasks_max": "1",
                    "field_mappings": {
                        "database.hostname": ["host"],
                        "database.port": ["port"],
                        "database.user": ["username"],
                        "database.password": ["password"],
                    },
                    "defaults": defaults or {},
                },
            }
        )
    )
    return str(root)


def _generate(connectors_dir, source_type="postgresql"):
    gen = CDCConfigGenerator(connectors_dir=connectors_dir)
    result = gen.generate_config(
        source_type=source_type,
        connection_config=CONNECTION_CONFIG,
        tables=["public.users"],
    )
    assert result.success, result.error
    return result


def _security_keys(config):
    return {k: v for k, v in config.items() if k.startswith(SECURITY_PREFIXES)}


# --------------------------------------------------------------------------
# The premise: metadata really is the path generate_config() prefers
# --------------------------------------------------------------------------

def test_metadata_template_is_the_path_generate_config_picks(tmp_path):
    """Without this the rest of the file could pass while testing the fallback."""
    assert _generate(_metadata_dir(tmp_path)).method_used == "metadata"


def test_builtin_template_is_used_when_no_metadata_declares_cdc(tmp_path):
    (tmp_path / "connectors").mkdir()
    assert _generate(str(tmp_path / "connectors")).method_used == "template"


# --------------------------------------------------------------------------
# Schema-history security on both paths
# --------------------------------------------------------------------------

def test_metadata_path_secures_both_halves_of_the_history_client(tmp_path, sasl_env):
    """The consumer half is the one that replays history on restart; a config
    carrying only the producer half works until the first restart."""
    config = _generate(_metadata_dir(tmp_path)).config
    for prefix in SECURITY_PREFIXES:
        assert config[prefix + "security.protocol"] == "SASL_SSL", prefix
        assert config[prefix + "sasl.mechanism"] == "SCRAM-SHA-512", prefix
        assert config[prefix + "sasl.jaas.config"].startswith(
            "org.apache.kafka.common.security.scram.ScramLoginModule required"
        ), prefix


def test_template_path_secures_both_halves_of_the_history_client(tmp_path, sasl_env):
    (tmp_path / "connectors").mkdir()
    config = _generate(str(tmp_path / "connectors")).config
    for prefix in SECURITY_PREFIXES:
        assert config[prefix + "security.protocol"] == "SASL_SSL", prefix
        assert config[prefix + "sasl.mechanism"] == "SCRAM-SHA-512", prefix


def test_both_paths_emit_the_same_security_block(tmp_path, sasl_env):
    """The regression guard: this fails the moment EITHER path drops or drifts
    from the shared debezium_schema_history_security() block."""
    (tmp_path / "plain").mkdir()
    from_metadata = _security_keys(_generate(_metadata_dir(tmp_path)).config)
    from_template = _security_keys(_generate(str(tmp_path / "plain")).config)

    assert from_metadata, "metadata path emitted no schema-history security at all"
    assert from_metadata == from_template


def test_plaintext_changes_neither_path(tmp_path):
    """An existing unsecured deployment's config must be unchanged."""
    (tmp_path / "plain").mkdir()
    assert _security_keys(_generate(_metadata_dir(tmp_path)).config) == {}
    assert _security_keys(_generate(str(tmp_path / "plain")).config) == {}


# --------------------------------------------------------------------------
# publication.autocreate.mode — the CDC-02 ordering invariant
# --------------------------------------------------------------------------

def test_metadata_path_disables_publication_autocreate(tmp_path):
    """The orchestrator creates the publication BEFORE the replication slot.
    Letting Debezium create it reverses that order and loses rows silently."""
    config = _generate(_metadata_dir(tmp_path)).config
    assert config["publication.autocreate.mode"] == "disabled"


def test_template_path_disables_publication_autocreate(tmp_path):
    (tmp_path / "connectors").mkdir()
    config = _generate(str(tmp_path / "connectors")).config
    assert config["publication.autocreate.mode"] == "disabled"


def test_a_template_asking_for_filtered_does_not_get_it(tmp_path):
    """metadata.json is connector-authored data, not a permission slip."""
    connectors = _metadata_dir(
        tmp_path, defaults={"publication.autocreate.mode": "filtered"}
    )
    assert _generate(connectors).config["publication.autocreate.mode"] == "disabled"


def test_a_postgres_derivative_is_recognised_by_its_connector_class(tmp_path):
    """A new PG derivative ships a metadata.json naming Debezium's Postgres
    connector class long before anyone adds its name to a family list, and the
    ordering invariant has to hold for it on day one."""
    connectors = _metadata_dir(tmp_path, source="neon")
    config = _generate(connectors, source_type="neon").config
    assert config["publication.autocreate.mode"] == "disabled"


def test_a_non_postgres_template_gets_no_publication_key(tmp_path):
    """publication.autocreate.mode is a PostgreSQL property; on a MySQL config
    Connect rejects it as an unknown configuration."""
    connectors = _metadata_dir(
        tmp_path,
        source="mysql",
        connector_class="io.debezium.connector.mysql.MySqlConnector",
    )
    config = _generate(connectors, source_type="mysql").config
    assert "publication.autocreate.mode" not in config
