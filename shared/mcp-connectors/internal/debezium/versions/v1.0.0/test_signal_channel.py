#!/usr/bin/env python3
"""Tests for the Kafka signal channel on PostgreSQL and MongoDB connectors.

Re-snapshot and "backfill newly added tables" both send Debezium an
execute-snapshot signal over this channel (orchestrator BackfillCDCTables). It
used to be wired only by the incremental snapshot strategy, which the
orchestrator picks for PostgreSQL loads of at least 1,000,000 rows — so every
smaller pipeline refused both controls with cdc_backfill_not_supported.

Two things must hold now that the orchestrator passes signal_kafka_topic for
every PostgreSQL connector:
  1. the channel alone never changes how the initial load runs, and
  2. the start_sync hint `incremental_snapshot` stays False for it — the
     orchestrator sends a history-loading signal when that hint is True, so a
     blocking-snapshot connector reporting True would load its history twice.

Run: python3 test_signal_channel.py
"""
import connector

SIGNAL_KEYS = {
    "signal.enabled.channels",
    "signal.kafka.topic",
    "signal.kafka.bootstrap.servers",
    "signal.kafka.groupId",
    "read.only",
}
TOPIC = "rsync.signals.abc12345"


def _args(db_type="postgresql", **extra):
    args = {
        "database_type": db_type,
        "connector_name": "cdc-abc12345",
        "db_host": "db.example.com",
        "db_user": "svc",
        "db_password": "pw",
        "db_name": "app",
        "tables": ["public.users"] if db_type == "postgresql" else ["app.users"],
        "cdc_mode": "initial",
        "snapshot_mode": "initial",
    }
    args.update(extra)
    return args


def _cfg(**kw):
    _, cfg, _ = connector.DebeziumConnector()._build_config(_args(**kw))
    return cfg


class _Resp:
    status_code = 201
    text = ""


class _Client:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def post(self, *a, **k):
        return _Resp()


def _start(**kw):
    srv = connector.DebeziumConnector()
    srv._client = lambda: _Client()  # never reach a real Kafka Connect
    out = srv.debezium_start_sync(_args(**kw))
    assert out.get("success") is True, out
    return out


def test_blocking_pg_gets_the_channel_and_nothing_else_changes():
    """The discriminating check: with and without the topic, the configs differ
    by exactly the signal keys — snapshot.mode included, so history still loads
    through the blocking initial snapshot, once."""
    without = _cfg()
    with_topic = _cfg(signal_kafka_topic=TOPIC)
    changed = {k for k in set(without) | set(with_topic) if without.get(k) != with_topic.get(k)}
    assert changed == SIGNAL_KEYS, f"channel changed more than the signal keys: {sorted(changed)}"
    assert with_topic["snapshot.mode"] == without["snapshot.mode"] != "no_data"
    assert with_topic["signal.enabled.channels"] == "kafka"
    assert with_topic["signal.kafka.topic"] == TOPIC
    assert with_topic["read.only"] == "true", "watermarks must come from the WAL, not a source table"


def test_no_topic_no_channel():
    """An orchestrator that predates this passes no topic; the connector must
    not invent one (that would be a channel nobody produces to)."""
    assert not SIGNAL_KEYS & set(_cfg())


def test_blocking_pg_hint_stays_false():
    out = _start(signal_kafka_topic=TOPIC)
    assert out["signal_topic"] == TOPIC
    assert out["incremental_snapshot"] is False, "a blocking connector would load its history twice"


def test_streaming_only_pg_hint_stays_false():
    """streaming_only also runs snapshot.mode=no_data — but has no history to
    load, so no_data + a signal topic must not read as incremental either."""
    out = _start(signal_kafka_topic=TOPIC, cdc_mode="streaming_only", snapshot_mode="streaming_only")
    assert out["config"]["snapshot.mode"] == "no_data"
    assert out["incremental_snapshot"] is False


def test_incremental_pg_unchanged():
    out = _start(signal_kafka_topic=TOPIC, snapshot_strategy="incremental")
    assert out["config"]["snapshot.mode"] == "no_data"
    assert out["config"]["signal.kafka.topic"] == TOPIC
    assert out["incremental_snapshot"] is True


def test_incremental_without_a_topic_is_refused():
    """The incremental strategy skips the blocking snapshot (no_data) and loads
    history only through a signal on this channel. This used to derive
    "rsync.signals.cdc-<id8>" when the caller named no topic: a topic nobody
    created or produced to. The result was a connector that streamed new changes,
    never loaded the table's existing rows, and looked healthy. The orchestrator
    always names the topic (cdcSignalTopicFor), so only a caller that did not
    reached the fallback. Such a caller now gets an error."""
    for db_type in ("postgresql", "mysql"):
        for missing in ({}, {"signal_kafka_topic": ""}, {"signal_kafka_topic": "   "}):
            try:
                cfg = _cfg(db_type=db_type, snapshot_strategy="incremental", **missing)
            except ValueError as e:
                assert "signal_kafka_topic" in str(e), e
            else:
                raise AssertionError(
                    f"{db_type} {missing!r}: incremental with no signal topic built a config "
                    f"listening on {cfg.get('signal.kafka.topic')!r}"
                )


def test_incremental_mysql_uses_the_callers_topic():
    cfg = _cfg(db_type="mysql", signal_kafka_topic=TOPIC, snapshot_strategy="incremental")
    assert cfg["snapshot.mode"] == "no_data"
    assert cfg["signal.kafka.topic"] == TOPIC


def test_mysql_topic_alone_wires_nothing():
    """MySQL's read-only watermarks need GTID mode; it keeps the source signal
    table the orchestrator falls back to."""
    assert not SIGNAL_KEYS & set(_cfg(db_type="mysql", signal_kafka_topic=TOPIC))


def test_mongodb_gets_the_channel_without_read_only():
    """MongoDB re-snapshots are BLOCKING only (an incremental one writes watermark
    documents into the source). The channel is the only change — no read.only,
    which is a relational-connector property — and the initial snapshot mode is
    untouched."""
    without = _cfg(db_type="mongodb")
    with_topic = _cfg(db_type="mongodb", signal_kafka_topic=TOPIC)
    changed = {k for k in set(without) | set(with_topic) if without.get(k) != with_topic.get(k)}
    assert changed == SIGNAL_KEYS - {"read.only"}, f"channel changed more than the signal keys: {sorted(changed)}"
    assert "read.only" not in with_topic
    assert with_topic["snapshot.mode"] == without["snapshot.mode"] != "no_data"
    assert with_topic["signal.kafka.topic"] == TOPIC
    assert with_topic["collection.include.list"] == "app.users"


def test_mongodb_no_topic_no_channel():
    assert not SIGNAL_KEYS & set(_cfg(db_type="mongodb"))


def test_mongodb_hint_stays_false():
    """The orchestrator sends a history-loading INCREMENTAL signal when this hint is
    True — which MongoDB must never receive."""
    out = _start(db_type="mongodb", signal_kafka_topic=TOPIC)
    assert out["incremental_snapshot"] is False


def test_mongodb_incremental_strategy_is_not_honoured():
    """Even if asked, MongoDB keeps its blocking initial snapshot: the incremental
    strategy would need a writable signal collection in the source."""
    cfg = _cfg(db_type="mongodb", signal_kafka_topic=TOPIC, snapshot_strategy="incremental")
    assert cfg["snapshot.mode"] != "no_data"
    assert "read.only" not in cfg


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
