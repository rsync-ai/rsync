#!/usr/bin/env python3
"""Tests for the topic.creation.* shape Connect creates CDC data topics with.

Without topic.creation.* the Connect worker does not create data topics at all:
the producer writes to a topic that does not exist and the BROKER auto-creates it
at its own num.partitions (1) and its own replication factor. Every CDC topic on a
three-broker cluster then had one partition, one leader, and one broker carrying
all of its traffic.

The orchestrator derives the numbers from the live broker list and passes them in.
This file pins that they are only emitted as a complete set (Connect ignores an
incomplete default group), that the floor travels with the factor, and that an
orchestrator which passes nothing leaves the config exactly as it was.

Run: python3 test_topic_creation_shape.py
"""
import connector

PARTITIONS = "topic.creation.default.partitions"
REPLICATION = "topic.creation.default.replication.factor"
MIN_ISR = "topic.creation.default.min.insync.replicas"


def _pg_config(**extra):
    args = {
        "database_type": "postgresql",
        "connector_name": "cdc-abc12345",
        "db_host": "db.example.com",
        "db_user": "svc",
        "db_password": "pw",
        "db_name": "app",
        "tables": ["public.users"],
    }
    args.update(extra)
    _, cfg, _ = connector.DebeziumConnector()._build_config(args)
    return cfg


def test_a_full_shape_reaches_connect():
    """The whole point: three brokers, three partitions, and a floor that fits."""
    cfg = _pg_config(
        topic_partitions=3,
        topic_replication_factor=3,
        topic_min_insync_replicas=2,
    )
    assert cfg[PARTITIONS] == "3"
    assert cfg[REPLICATION] == "3"
    assert cfg[MIN_ISR] == "2"


def test_nothing_passed_leaves_auto_create_alone():
    """An older orchestrator, or one that could not read the broker count, must
    change nothing — not emit a confident wrong number."""
    cfg = _pg_config()
    assert PARTITIONS not in cfg
    assert REPLICATION not in cfg
    assert MIN_ISR not in cfg


def test_partitions_without_a_replication_factor_emit_nothing():
    """Connect requires both for the default group and ignores an incomplete one.
    Emitting half of it would read, in the config, as a setting that is in force."""
    cfg = _pg_config(topic_partitions=3)
    assert PARTITIONS not in cfg
    assert REPLICATION not in cfg


def test_a_replication_factor_without_partitions_emits_nothing():
    """The mirror of the case above, so neither half can be shipped on its own."""
    cfg = _pg_config(topic_replication_factor=3)
    assert PARTITIONS not in cfg
    assert REPLICATION not in cfg


def test_the_floor_is_optional_but_the_pair_is_not():
    """A caller that states no min.insync.replicas still gets the pair; the broker
    default then applies to the floor, which is the pre-existing behaviour."""
    cfg = _pg_config(topic_partitions=2, topic_replication_factor=2)
    assert cfg[PARTITIONS] == "2"
    assert cfg[REPLICATION] == "2"
    assert MIN_ISR not in cfg


def test_zero_and_junk_are_not_shapes():
    """A malformed value must fall back to "say nothing" rather than reach Connect,
    which rejects partitions=0 outright and would fail every topic creation."""
    for bad in (0, -1, "", "three", None):
        cfg = _pg_config(topic_partitions=bad, topic_replication_factor=3)
        assert PARTITIONS not in cfg, f"partitions={bad!r} reached Connect"
        cfg = _pg_config(topic_partitions=3, topic_replication_factor=bad)
        assert REPLICATION not in cfg, f"replication={bad!r} reached Connect"


def test_strings_are_accepted_because_json_rpc_may_send_them():
    """The orchestrator sends ints, but the MCP boundary is JSON and a caller by
    hand sends strings. Both must produce the same config."""
    cfg = _pg_config(
        topic_partitions="3",
        topic_replication_factor="3",
        topic_min_insync_replicas="2",
    )
    assert (cfg[PARTITIONS], cfg[REPLICATION], cfg[MIN_ISR]) == ("3", "3", "2")


def test_an_explicit_override_still_wins():
    """connector_config_overrides is applied after the shape, so an operator
    debugging a cluster can still force a count by hand."""
    cfg = _pg_config(
        topic_partitions=3,
        topic_replication_factor=3,
        connector_config_overrides={PARTITIONS: "6"},
    )
    assert cfg[PARTITIONS] == "6"
    # The control: the rest of the shape is untouched, so the override is what
    # changed the value rather than the shape failing to apply at all.
    assert cfg[REPLICATION] == "3"


def test_mongodb_gets_the_same_shape():
    """The shape is connector-class independent — it is a Connect property, not a
    Debezium one — so a MongoDB source must not quietly miss out on it."""
    _, cfg, _ = connector.DebeziumConnector()._build_config(
        {
            "database_type": "mongodb",
            "connector_name": "cdc-mongo123",
            "connection_string": "mongodb://db:27017/?replicaSet=rs0",
            "db_name": "shop",
            "tables": ["shop.orders"],
            "topic_partitions": 3,
            "topic_replication_factor": 3,
        }
    )
    assert cfg[PARTITIONS] == "3"
    assert cfg[REPLICATION] == "3"


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
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"ERROR {fn.__name__}: {type(e).__name__}: {e}")
    print(f"\n{len(fns) - failed}/{len(fns)} passed")
    raise SystemExit(1 if failed else 0)
