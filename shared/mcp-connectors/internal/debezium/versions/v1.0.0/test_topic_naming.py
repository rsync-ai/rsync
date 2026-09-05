#!/usr/bin/env python3
"""Pins this connector's topic namespacing to the cross-language contract.

Debezium writes the CDC data topics, so what this file computes for topic.prefix
IS what lands on the customer's cluster -- and the Go sink subscribes to names it
predicts independently. If the two disagree the sink drains nothing and reports
no error, so the agreement is checked here rather than assumed.

Run: python3 -m pytest test_topic_naming.py
"""
import json
import os
import pathlib

import connector

CONTRACT = pathlib.Path(__file__).resolve().parents[5] / "contracts" / "kafka-topic-naming.json"


def _cases():
    assert CONTRACT.is_file(), f"shared contract not found at {CONTRACT}"
    cases = json.loads(CONTRACT.read_text())["cases"]
    assert cases, "the shared contract has no cases; this would pass vacuously"
    return cases


def test_qualify_topic_matches_cross_language_contract():
    original = os.environ.get("KAFKA_TOPIC_PREFIX")
    try:
        for case in _cases():
            if case["prefix"] is None:
                os.environ.pop("KAFKA_TOPIC_PREFIX", None)
            else:
                os.environ["KAFKA_TOPIC_PREFIX"] = case["prefix"]
            got = connector._qualify_topic(case["input"])
            assert got == case["want"], (
                f"prefix={case['prefix']!r} _qualify_topic({case['input']!r}) "
                f"= {got!r}, want {case['want']!r}"
            )
    finally:
        if original is None:
            os.environ.pop("KAFKA_TOPIC_PREFIX", None)
        else:
            os.environ["KAFKA_TOPIC_PREFIX"] = original


def test_default_namespace_is_rsync():
    """A customer listing topics on their own cluster must be able to tell which
    ones this product created. An unset variable must still produce the prefix."""
    original = os.environ.get("KAFKA_TOPIC_PREFIX")
    try:
        os.environ.pop("KAFKA_TOPIC_PREFIX", None)
        assert connector._qualify_topic("cdc-abc12345").startswith("rsync")
        assert connector._topic_prefix() == "rsync."
    finally:
        if original is not None:
            os.environ["KAFKA_TOPIC_PREFIX"] = original


# --- schema-history topic --------------------------------------------------
#
# The schema-history topic is the one topic in the CDC data plane that nothing
# created. Debezium's default is publication-by-produce: Connect writes to
# schema.history.internal.kafka.topic and lets the broker bring it into being.
# That works on a broker with auto.create.topics.enable=true and fails on a
# customer's, which usually has it off -- and it fails in the worst available
# shape, because the history is only READ on connector RESTART. A pipeline
# provisions, snapshots, streams for days, then dies on a restart naming a topic
# nobody can find a creator for.
#
# The orchestrator now pre-creates it (executor.go schemaHistoryTopicFor) with a
# geometry Debezium requires -- 1 partition, cleanup.policy=delete,
# retention.ms=-1 -- and passes the name it used in schema_history_topic. These
# tests pin the three things that can silently undo that:
#
#   1. an explicit name is used VERBATIM (else the orchestrator creates one topic
#      with correct retention and Connect writes to a different, auto-created one,
#      and the failure just moves back to the first restart while looking fixed),
#   2. an already-qualified name is not prefixed twice, and
#   3. omitting it is byte-identical to the pre-change behaviour, so a direct
#      caller -- a test, an operator hitting the MCP by hand, an older
#      orchestrator that does not send the field -- keeps working.
#
# The Go side of the same contract is
# backend-orchestrator/internal/agents/executor/cdc_schema_history_topic_test.go;
# the _safe_name cases below are mirrored there case-for-case.

_PG_ARGS = {
    "database_type": "postgresql",
    "db_host": "db.example.com",
    "db_user": "FAKEPLACEHOLDER",
    "db_password": "FAKEPLACEHOLDER",
    "db_name": "appdb",
    "tables": ["public.orders"],
}


def _history_topic(**overrides):
    args = dict(_PG_ARGS)
    args.update(overrides)
    _name, cfg, _db = connector.DebeziumConnector()._build_config(args)
    return cfg["schema.history.internal.kafka.topic"]


def test_schema_history_topic_uses_the_caller_supplied_name_verbatim():
    original = os.environ.get("KAFKA_TOPIC_PREFIX")
    try:
        os.environ.pop("KAFKA_TOPIC_PREFIX", None)
        got = _history_topic(
            connector_name="cdc-abc12345",
            schema_history_topic="rsync.schemahistory.cdc-abc12345",
        )
        assert got == "rsync.schemahistory.cdc-abc12345", (
            f"got {got!r} -- the orchestrator pre-created that exact topic with "
            "retention.ms=-1 and cleanup.policy=delete; writing to any other name "
            "puts the history on an auto-created topic with the broker's defaults"
        )
        # And it really does override the derived fallback, rather than coinciding
        # with it: a name that could never be derived must still come through.
        assert (
            _history_topic(
                connector_name="cdc-abc12345",
                schema_history_topic="rsync.history.chosen-by-the-caller",
            )
            == "rsync.history.chosen-by-the-caller"
        )
    finally:
        if original is not None:
            os.environ["KAFKA_TOPIC_PREFIX"] = original


def test_schema_history_topic_is_not_prefixed_twice():
    """_qualify_topic is idempotent, and the caller's name arrives already
    qualified (the orchestrator mints it through kafkaclient.Topic). Prefixing it
    again yields rsync.rsync.schemahistory.* -- a second topic, created by nobody."""
    original = os.environ.get("KAFKA_TOPIC_PREFIX")
    try:
        os.environ.pop("KAFKA_TOPIC_PREFIX", None)
        assert (
            _history_topic(
                connector_name="cdc-abc12345",
                schema_history_topic="rsync.schemahistory.cdc-abc12345",
            )
            == "rsync.schemahistory.cdc-abc12345"
        )
        os.environ["KAFKA_TOPIC_PREFIX"] = "acme"
        assert (
            _history_topic(
                connector_name="cdc-abc12345",
                schema_history_topic="acme.schemahistory.cdc-abc12345",
            )
            == "acme.schemahistory.cdc-abc12345"
        )
        # A bare name under a custom prefix still gets qualified once.
        assert (
            _history_topic(
                connector_name="cdc-abc12345",
                schema_history_topic="schemahistory.cdc-abc12345",
            )
            == "acme.schemahistory.cdc-abc12345"
        )
    finally:
        if original is None:
            os.environ.pop("KAFKA_TOPIC_PREFIX", None)
        else:
            os.environ["KAFKA_TOPIC_PREFIX"] = original


def test_schema_history_topic_falls_back_to_the_pre_change_name():
    """Absent, empty and whitespace-only must all reproduce exactly what this
    connector computed before the field existed."""
    original = os.environ.get("KAFKA_TOPIC_PREFIX")
    try:
        os.environ.pop("KAFKA_TOPIC_PREFIX", None)
        want = connector._qualify_topic(
            f"schemahistory.{connector._safe_name('cdc-abc12345', 80)}"
        )
        assert want == "rsync.schemahistory.cdc-abc12345", (
            f"the pre-change name itself changed ({want!r}); every deployed "
            "connector's existing history topic is now orphaned"
        )
        for omitted in ({}, {"schema_history_topic": ""}, {"schema_history_topic": "   "},
                        {"schema_history_topic": None}):
            got = _history_topic(connector_name="cdc-abc12345", **omitted)
            assert got == want, f"{omitted!r} gave {got!r}, want {want!r}"
    finally:
        if original is not None:
            os.environ["KAFKA_TOPIC_PREFIX"] = original


def test_safe_name_rules_match_the_go_copy():
    """debeziumSafeName() in backend-orchestrator/internal/agents/executor/executor.go
    is a hand-written copy of _safe_name. The trap it exists to avoid: the legal set
    here is [a-z0-9_-] and does NOT include the dot, even though the dot is legal in
    a Kafka topic name and every other naming helper in the repo keeps it. A
    connector name carrying a dot becomes an underscore here and would stay a dot in
    a naive reimplementation -- two topics differing by one character."""
    cases = [
        ("cdc-3a7e63e5", 80, "cdc-3a7e63e5"),
        ("CDC-3A7E63E5", 80, "cdc-3a7e63e5"),
        ("  cdc-3a7e63e5  ", 80, "cdc-3a7e63e5"),
        ("cdc.pipeline.7f2", 80, "cdc_pipeline_7f2"),
        ("cdc//pipeline!!7f2", 80, "cdc_pipeline_7f2"),
        ("cdc__pipeline___7f2", 80, "cdc_pipeline_7f2"),
        ("__cdc-7f2__", 80, "cdc-7f2"),
        ("!!!", 80, "rsync"),
        ("", 80, "rsync"),
        ("   ", 80, "rsync"),
        ("abcdefghij", 4, "abcd"),
    ]
    assert cases, "no cases; this would pass vacuously"
    for raw, max_len, want in cases:
        got = connector._safe_name(raw, max_len)
        assert got == want, f"_safe_name({raw!r}, {max_len}) = {got!r}, want {want!r}"


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failed = 0
    for fn in fns:
        try:
            fn()
            print(f"PASS {fn.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"FAIL {fn.__name__}: {e}")
    print(f"\n{len(fns) - failed}/{len(fns)} passed")
    raise SystemExit(1 if failed else 0)
