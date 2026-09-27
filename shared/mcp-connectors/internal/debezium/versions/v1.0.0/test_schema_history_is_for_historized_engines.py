#!/usr/bin/env python3
"""Only historized engines get a schema-history topic and a DDL topic.

A historized Debezium connector (MySQL/MariaDB, SQL Server, Oracle) replays a
schema-history topic on restart to rebuild its table schemas. With
include.schema.changes on, it also publishes source DDL to the bare
topic.prefix topic. PostgreSQL decodes the WAL against the live catalog and
MongoDB has no relational schema, so neither one reads a history topic or emits
DDL.

This config used to set include.schema.changes and every
schema.history.internal.* key on every engine. MongoDB stripped them again; PG
kept them. So each PG pipeline named a schema-history topic that the
orchestrator pre-created and that no connector ever wrote.

The orchestrator mirrors the set here when it decides which topics to
pre-create for a pipeline. connector._HISTORIZED_ENGINES is the one list; the
first test pins it.

Run: python3 -m pytest test_schema_history_is_for_historized_engines.py
"""
import pytest

import connector

HISTORY_TOPIC = "rsync.schemahistory.cdc-abc12345"

_KAFKA_ENV = (
    "KAFKA_SECURITY_PROTOCOL",
    "KAFKA_SASL_MECHANISM",
    "KAFKA_SASL_USERNAME",
    "KAFKA_SASL_PASSWORD",
    "KAFKA_SSL_CA_LOCATION",
    "KAFKA_SSL_KEYSTORE_LOCATION",
    "KAFKA_SSL_SKIP_VERIFY",
    "KAFKA_BROKERS",
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_TOPIC_PREFIX",
)

_TABLES = {
    "postgresql": ["public.users"],
    "postgres": ["public.users"],
    "mongodb": ["app.users"],
    "mysql": ["app.users"],
    "mariadb": ["app.users"],
    "sqlserver": ["dbo.users"],
    "mssql": ["dbo.users"],
    "oracle": ["APP.USERS"],
}


@pytest.fixture
def sasl(monkeypatch):
    """A SASL cluster, so the schema-history security properties are in play
    too: they are schema.history.* keys and must follow the same rule."""
    for k in _KAFKA_ENV:
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("KAFKA_BROKERS", "kafka:9092")
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "rsync")
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "FAKEPLACEHOLDER")


def _cfg(db_type, **extra):
    args = {
        "database_type": db_type,
        "connector_name": "cdc-abc12345",
        "db_host": "db.example.com",
        "db_user": "svc",
        "db_password": "FAKEPLACEHOLDER",
        "db_name": "app",
        "tables": _TABLES[db_type],
        # The orchestrator sends this today for every engine. A non-historized
        # engine must ignore it rather than name a topic nobody writes.
        "schema_history_topic": HISTORY_TOPIC,
    }
    args.update(extra)
    _, cfg, _ = connector.DebeziumConnector()._build_config(args)
    return cfg


def _history_keys(cfg):
    return sorted(k for k in cfg if k.startswith("schema.history.") or k == "include.schema.changes")


def test_the_historized_set_is_decided_for_every_supported_engine():
    supported = set(connector.DebeziumConnector().supported)
    assert connector._HISTORIZED_ENGINES == {"mysql", "sqlserver", "oracle"}
    assert connector._HISTORIZED_ENGINES <= supported
    # A new engine lands in neither list by accident. It has to be added to one.
    assert supported - connector._HISTORIZED_ENGINES == {"postgresql", "mongodb"}


@pytest.mark.parametrize("db_type", ["postgresql", "postgres", "mongodb"])
def test_a_non_historized_engine_carries_no_history_or_ddl_keys(sasl, db_type):
    cfg = _cfg(db_type)
    assert _history_keys(cfg) == [], f"{db_type} names a schema history / DDL topic: {_history_keys(cfg)}"
    assert HISTORY_TOPIC not in cfg.values()


def test_mongodb_strips_history_and_ddl_keys_a_caller_override_puts_back(sasl):
    """The base config no longer sets these keys for MongoDB, so the only way
    they reach a MongoDB connector is connector_config_overrides, which
    _build_config applies before the MongoDB branch. Connect rejects a MongoDB
    config that carries any of them, so the MongoDB branch must strip them."""
    cfg = _cfg(
        "mongodb",
        connector_config_overrides={
            "schema.history.internal.kafka.topic": "x",
            "schema.history.internal.producer.security.protocol": "SASL_SSL",
            "include.schema.changes": "true",
        },
    )
    assert _history_keys(cfg) == [], f"mongodb kept override keys: {_history_keys(cfg)}"


@pytest.mark.parametrize("db_type", ["mysql", "mariadb", "sqlserver", "mssql", "oracle"])
def test_a_historized_engine_keeps_its_history_and_ddl_topics(sasl, db_type):
    cfg = _cfg(db_type)
    assert cfg["include.schema.changes"] == "true", "the DDL topic feeds the cdcstats schema-change consumer"
    assert cfg["schema.history.internal.kafka.topic"] == HISTORY_TOPIC
    assert cfg["schema.history.internal.kafka.bootstrap.servers"] == "kafka:9092"
    for role in ("producer", "consumer"):
        assert cfg[f"schema.history.internal.{role}.security.protocol"] == "SASL_SSL", (
            f"{db_type}: the history client would fail on the first restart of a SASL cluster"
        )


def test_a_historized_engine_derives_the_history_topic_when_none_is_passed(sasl):
    cfg = _cfg("mysql", schema_history_topic="")
    assert cfg["schema.history.internal.kafka.topic"] == "rsync.schemahistory.cdc-abc12345"


def test_an_explicit_override_still_wins_on_a_historized_engine(sasl):
    cfg = _cfg(
        "mysql",
        connector_config_overrides={"schema.history.internal.kafka.topic": "rsync.history.by-hand"},
    )
    assert cfg["schema.history.internal.kafka.topic"] == "rsync.history.by-hand"


def test_the_ddl_topic_is_the_topic_prefix_for_every_engine(sasl):
    """The bare DDL topic of a historized engine IS topic.prefix, so the
    orchestrator pre-creates it under the name it already derives for the data
    topics (debeziumTopicPrefixFor). One naming rule for every engine."""
    for db_type in _TABLES:
        assert _cfg(db_type)["topic.prefix"] == "rsync.cdc-abc12345", db_type
