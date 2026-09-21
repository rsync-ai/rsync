#!/usr/bin/env python3
"""A Debezium SOURCE must bound its retries, and a MongoDB source must heartbeat.

KI-CDC-MONGO-RESUME-TOKEN-SILENT-STALL. Two independent defaults combined into a
pipeline that reported "healthy" for three days while moving zero rows:

1. Debezium only commits a FRESH resume token when it emits an event from a
   captured collection. A source with no writes therefore keeps re-committing the
   SAME token while the oplog rolls forward, and the next reconnect is refused with
   ChangeStreamHistoryLost (server error 286, NonResumableChangeStreamError). The
   trigger is IDLENESS, not downtime, which is why a bigger oplog only widens the
   window instead of closing it. `heartbeat.interval.ms` closes it: the connector
   commits a fresh token on a timer whether or not the source is writing.

2. Debezium's default `errors.max.retries` is -1 — retry a retriable error forever.
   The task never leaves RUNNING, Connect's /status stays green, and every piece of
   machinery rsync already has for this failure (diagnose.go -> ActionReSnapshot ->
   MONGODB_RESUME_TOKEN_INVALID) hangs off a task reaching FAILED, so none of it
   ever ran. Measured on the live connector: 0 FAILED transitions in 24h while the
   error fired roughly ten times a minute.

This file is the live path. `cdc_config_generator.py` also emits a MongoDB config
but is explicitly advisory; the config that actually starts a connector is the one
`_build_config` returns here.

Run: python3 -m pytest test_heartbeat_and_retry_bounds.py
"""
import os
import pathlib

import pytest

import connector

REPO_ROOT = pathlib.Path(__file__).resolve().parents[6]
EXECUTOR_GO = REPO_ROOT / "backend-orchestrator" / "internal" / "agents" / "executor" / "executor.go"

# Every environment variable that can move a value this file asserts on. Connector
# suites in this directory share one pytest process and several of them set Kafka
# env vars without restoring them, so the state each test starts from is whatever
# the previous file left behind unless it is cleared here.
_OWNED_ENV = (
    "KAFKA_TOPIC_PREFIX",
    "CDC_CONNECTOR_MAX_RETRIES",
    "CDC_CONNECTOR_RETRY_WAIT_MS",
    "CDC_MONGO_HEARTBEAT_INTERVAL_MS",
)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for name in _OWNED_ENV:
        monkeypatch.delenv(name, raising=False)


def _cfg(**overrides):
    """The config `_build_config` actually hands to Kafka Connect."""
    args = {
        "database_type": "mongodb",
        "connector_name": "cdc-3a7e63e5",
        "db_host": "mongo",
        "db_user": "u",
        "db_password": "p",
        "db_name": "shop",
        "tables": ["orders"],
    }
    args.update(overrides)
    _, cfg, _ = connector.DebeziumConnector()._build_config(args)
    return cfg


def _relational_cfg(db_type, **overrides):
    args = {
        "database_type": db_type,
        "connector_name": f"cdc-{db_type}-1",
        "db_host": "db",
        "db_user": "u",
        "db_password": "p",
        "db_name": "shop",
        "tables": ["public.orders"] if db_type == "postgresql" else ["shop.orders"],
    }
    args.update(overrides)
    _, cfg, _ = connector.DebeziumConnector()._build_config(args)
    return cfg


# --------------------------------------------------------------------------
# Heartbeats (MongoDB only)
# --------------------------------------------------------------------------


def test_mongodb_source_heartbeats_by_default():
    cfg = _cfg()
    assert cfg["heartbeat.interval.ms"] == "300000"
    # topic.heartbeat.prefix is the key that NAMES the topic; heartbeat.topics.prefix
    # is the legacy twin kept in lockstep. See the two-key test below.
    assert cfg["topic.heartbeat.prefix"] == "rsync.heartbeat"
    assert cfg["heartbeat.topics.prefix"] == "rsync.heartbeat"


def test_the_key_debezium_names_the_topic_from_is_set():
    """KI-CDC-HEARTBEAT-TOPIC-PREFIX-KEY-IGNORED — the regression test this file lacked.

    There are TWO heartbeat-prefix keys and only one of them names the topic:

      * `heartbeat.topics.prefix` is a live, non-deprecated ConfigDef entry
        (io/debezium/heartbeat/Heartbeat.java), so setting only it validates cleanly
        and looks correct — but it is vestigial for naming.
      * `topic.heartbeat.prefix` is what the topic-naming strategy reads
        (io/debezium/schema/AbstractTopicNamingStrategy.java), default
        `__debezium-heartbeat`.

    #1098 set only the first. Heartbeats fired on schedule and landed on
    `__debezium-heartbeat.<topic.prefix>`, while the pre-created, ACL-granted
    `rsync.heartbeat.<topic.prefix>` stayed at offset 0 forever (observed live
    2026-09-20). On a BYO-Kafka cluster that grants rsync only `rsync.*`, the
    unqualified topic falls outside the grant and — with errors.tolerance=none —
    fails the task outright.

    Every other assertion in this file checks the key rsync WRITES. This one checks
    the key Debezium READS, which is the only reason the wrong fix could not stay
    green.
    """
    cfg = _cfg()
    assert "topic.heartbeat.prefix" in cfg, (
        "the connector sets no topic.heartbeat.prefix, so Debezium will name the "
        "heartbeat topic __debezium-heartbeat.<topic.prefix> — outside the rsync.* "
        "ACL grant and not the topic executor.go pre-creates"
    )
    assert cfg["topic.heartbeat.prefix"] == cfg["heartbeat.topics.prefix"], (
        "the two heartbeat-prefix keys disagree; whichever one a given Debezium "
        "version honours, the other names a topic nothing created"
    )
    assert not cfg["topic.heartbeat.prefix"].startswith("__"), (
        "an unqualified __debezium-heartbeat prefix is exactly the "
        "KI-KAFKA-DATAPLANE-AUTOCREATE-ONLY failure mode"
    )


def test_heartbeat_interval_is_a_positive_integer_of_minutes_not_hours():
    # The freshness watchdog (cdc_source_freshness.go) alarms after 20 minutes of a
    # frozen stream position. An interval at or above that window would make a
    # HEALTHY idle pipeline alarm, so the two numbers are not independent.
    ms = int(cfg_interval := _cfg()["heartbeat.interval.ms"])
    assert ms > 0, cfg_interval
    assert ms <= 10 * 60 * 1000, (
        f"heartbeat.interval.ms={ms} is not comfortably inside the 20m freshness "
        "window in cdc_source_freshness.go; an idle-but-healthy pipeline would alarm"
    )


@pytest.mark.parametrize("db_type", ["postgresql", "mysql", "sqlserver"])
def test_relational_sources_get_no_heartbeat(db_type):
    # Deliberately MongoDB-only. A PostgreSQL replication slot pins WAL on the
    # server, so an idle Postgres source cannot lose its position the way a capped
    # oplog does. MySQL's time-based binlog expiry is the same class of risk and is
    # tracked in BACKLOG.md rather than changed blind here. Enabling heartbeats for
    # a relational source would also mean a topic nothing pre-creates.
    cfg = _relational_cfg(db_type)
    assert "heartbeat.interval.ms" not in cfg
    assert "topic.heartbeat.prefix" not in cfg
    assert "heartbeat.topics.prefix" not in cfg


def test_heartbeat_prefix_is_namespaced_under_a_custom_kafka_topic_prefix(monkeypatch):
    # A BYO-Kafka cluster grants rsync only `<prefix>*`. An unqualified
    # `__debezium-heartbeat.*` topic is refused by the ACL, which is precisely
    # KI-KAFKA-DATAPLANE-AUTOCREATE-ONLY.
    monkeypatch.setenv("KAFKA_TOPIC_PREFIX", "acme.")
    cfg = _cfg()
    assert cfg["topic.heartbeat.prefix"] == "acme.heartbeat"
    assert cfg["heartbeat.topics.prefix"] == "acme.heartbeat"


def test_heartbeat_prefix_qualification_is_idempotent(monkeypatch):
    # _qualify_topic must not double-prefix a name that already carries it —
    # otherwise the orchestrator pre-creates rsync.heartbeat.* and the connector
    # publishes to rsync.rsync.heartbeat.*, which no ACL covers.
    monkeypatch.setenv("KAFKA_TOPIC_PREFIX", "rsync.")
    cfg = _cfg()
    assert cfg["topic.heartbeat.prefix"] == "rsync.heartbeat"
    assert cfg["heartbeat.topics.prefix"] == "rsync.heartbeat"


def test_orchestrator_supplied_prefix_wins_over_the_default():
    # executor.go passes heartbeat_topics_prefix so the name is derived ONCE. Two
    # independent derivations that disagree would have the orchestrator create one
    # topic and Connect publish to another.
    cfg = _cfg(heartbeat_topics_prefix="rsync.heartbeat")
    assert cfg["topic.heartbeat.prefix"] == "rsync.heartbeat"
    assert cfg["heartbeat.topics.prefix"] == "rsync.heartbeat"


def test_an_explicit_config_override_still_wins_over_the_heartbeat_defaults():
    cfg = _cfg(
        connector_config_overrides={
            "heartbeat.interval.ms": "60000",
            "topic.heartbeat.prefix": "rsync.custom",
            "heartbeat.topics.prefix": "rsync.custom",
        }
    )
    assert cfg["heartbeat.interval.ms"] == "60000"
    assert cfg["topic.heartbeat.prefix"] == "rsync.custom"
    assert cfg["heartbeat.topics.prefix"] == "rsync.custom"


def test_heartbeat_interval_env_override(monkeypatch):
    monkeypatch.setenv("CDC_MONGO_HEARTBEAT_INTERVAL_MS", "60000")
    assert _cfg()["heartbeat.interval.ms"] == "60000"


def test_blank_heartbeat_env_falls_back_to_the_shipped_default(monkeypatch):
    # An unset variable and a variable set to whitespace are the same intent. An
    # empty heartbeat.interval.ms would be rejected by Connect at PUT time.
    monkeypatch.setenv("CDC_MONGO_HEARTBEAT_INTERVAL_MS", "   ")
    assert _cfg()["heartbeat.interval.ms"] == "300000"


# --------------------------------------------------------------------------
# Retry bounds (every source)
# --------------------------------------------------------------------------


@pytest.mark.parametrize("db_type", ["postgresql", "mysql", "sqlserver"])
def test_every_relational_source_bounds_its_retries(db_type):
    cfg = _relational_cfg(db_type)
    assert cfg["errors.max.retries"] == "30"
    assert cfg["retriable.restart.connector.wait.ms"] == "10000"


def test_mongodb_bounds_its_retries():
    cfg = _cfg()
    assert cfg["errors.max.retries"] == "30"
    assert cfg["retriable.restart.connector.wait.ms"] == "10000"


@pytest.mark.parametrize("db_type", ["postgresql", "mysql", "sqlserver", "mongodb"])
def test_the_bound_uses_debeziums_retry_loop_not_connects_error_handler(db_type):
    """`errors.retry.*` is a different subsystem and would be a no-op here.

    Both families read like they pace a retry, but only one is Debezium's.
    Confirmed against the live plugin ConfigDef (PUT /connector-plugins/
    io.debezium.connector.mongodb.MongoDbConnector/config/validate on Kafka Connect
    3.9.2):

      * errors.max.retries                 group "Connector"      (Debezium)
      * retriable.restart.connector.wait.ms group "Connector"      (Debezium)
      * errors.retry.delay.max.ms           group "Error Handling" (Connect)
      * errors.retry.timeout                group "Error Handling", effective '0'

    Connect's RetryWithToleranceOperator is gated by errors.retry.timeout, whose
    default of 0 means "no retries will be attempted". Setting errors.retry.delay.max.ms
    therefore changes the backoff cap of a retry path that never runs — it looks like a
    retry bound in a diff and bounds nothing. (errors.retry.delay.initial.ms is not in
    this plugin's ConfigDef at all.) This test exists because that substitution is the
    natural mistake, and it was made once here already.
    """
    cfg = _cfg() if db_type == "mongodb" else _relational_cfg(db_type)
    assert cfg["retriable.restart.connector.wait.ms"] == "10000"
    for wrong in ("errors.retry.delay.max.ms", "errors.retry.delay.initial.ms", "errors.retry.timeout"):
        assert wrong not in cfg, (
            f"{wrong} belongs to Kafka Connect's error handler, not Debezium's retry "
            "loop; setting it does not bound anything"
        )


def test_the_retry_bound_is_not_debeziums_unlimited_default():
    # The whole point. -1 is what shipped, and -1 is what kept a permanently broken
    # task in RUNNING for three days.
    assert _cfg()["errors.max.retries"] != "-1"
    assert int(_cfg()["errors.max.retries"]) > 0


def test_retry_bound_env_override(monkeypatch):
    monkeypatch.setenv("CDC_CONNECTOR_MAX_RETRIES", "5")
    monkeypatch.setenv("CDC_CONNECTOR_RETRY_WAIT_MS", "2000")
    cfg = _cfg()
    assert cfg["errors.max.retries"] == "5"
    assert cfg["retriable.restart.connector.wait.ms"] == "2000"


def test_minus_one_restores_the_old_infinite_retry_behaviour(monkeypatch):
    # The documented escape hatch. An operator who would rather have a pipeline
    # retry forever than fail must be able to say so without editing an image.
    monkeypatch.setenv("CDC_CONNECTOR_MAX_RETRIES", "-1")
    assert _cfg()["errors.max.retries"] == "-1"


def test_an_explicit_config_override_still_wins_over_the_retry_defaults():
    cfg = _cfg(connector_config_overrides={"errors.max.retries": "3"})
    assert cfg["errors.max.retries"] == "3"


# --------------------------------------------------------------------------
# Cross-language agreement with the orchestrator
# --------------------------------------------------------------------------


def test_go_and_python_agree_on_the_bare_heartbeat_prefix():
    """executor.go pre-creates the topic; this file names it. They must match.

    The orchestrator normally passes heartbeat_topics_prefix explicitly, so a
    divergence is invisible on that path — but a connector started straight through
    MCP, or by an orchestrator older than the parameter, falls back to the default
    below and would publish to a topic nothing created. The Go side qualifies the
    same bare string through kafkaclient.Topic().
    """
    assert EXECUTOR_GO.is_file(), f"executor.go not found at {EXECUTOR_GO}"
    src = EXECUTOR_GO.read_text()
    want = f'return kafkaclient.Topic("{connector._DEFAULT_HEARTBEAT_TOPICS_PREFIX}")'
    assert want in src, (
        f"heartbeatTopicsPrefix() in executor.go does not qualify "
        f"{connector._DEFAULT_HEARTBEAT_TOPICS_PREFIX!r}; the orchestrator would "
        f"pre-create one topic and this connector would publish to another"
    )


def test_the_heartbeat_topic_composes_prefix_first():
    """Debezium names the topic <heartbeat.topics.prefix>.<topic.prefix>.

    The prefix comes FIRST, which is the opposite of what the property name reads
    like, and getting it backwards yields a plausible name that nothing created.
    Pinned here against the Go composition so both sides state the same rule.
    """
    cfg = _cfg()
    hb_prefix = cfg["topic.heartbeat.prefix"]
    topic_prefix = cfg["topic.prefix"]
    assert f"{hb_prefix}.{topic_prefix}" == "rsync.heartbeat.rsync.cdc-3a7e63e5"

    src = EXECUTOR_GO.read_text()
    assert "return heartbeatTopicsPrefix() + \".\" + topicPrefix" in src, (
        "heartbeatTopicFor() in executor.go no longer composes the heartbeat "
        "prefix before the topic prefix"
    )


def test_topic_prefix_keeps_dots_so_the_heartbeat_topic_is_predictable():
    # topic.prefix is NOT run through _safe_name (unlike schema_history_topic), so
    # a dotted connector name survives into the topic name. The Go helper does the
    # same. Pinned because "make it consistent" is the obvious wrong fix.
    cfg = _cfg(connector_name="cdc.with.dots")
    assert cfg["topic.prefix"] == "rsync.cdc.with.dots"


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
