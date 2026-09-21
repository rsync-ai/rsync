#!/usr/bin/env python3
"""Two pipelines on one database server must not share a CDC identity.

A company with many databases on one server gets one connection (and one
pipeline) per database, and often several pipelines on the same database. The
orchestrator names each connector cdc-<pipeline id>; everything Debezium owns
on the source and in Kafka is derived from that name here. If any of it were
derived from the database instead, the second pipeline would silently steal the
first one's stream: MySQL drops one of two binlog readers that share a server
id, and two readers of one replication slot each get half the changes.

Run: python3 test_pipeline_identity.py
"""
import connector


def _cfg(db_type, connector_name, db_name, tables):
    _, cfg, _ = connector.DebeziumConnector()._build_config(
        {
            "database_type": db_type,
            "connector_name": connector_name,
            "db_host": "db.internal",
            "db_user": "svc",
            "db_password": "pw",
            "db_name": db_name,
            "tables": tables,
        }
    )
    return cfg


IDENTITY_KEYS = {
    "mysql": ["database.server.id", "topic.prefix", "schema.history.internal.kafka.topic"],
    "postgresql": ["slot.name", "publication.name", "topic.prefix"],
}


def test_two_pipelines_on_one_database_get_distinct_identities():
    for db_type, tables in (("mysql", ["app.users"]), ("postgresql", ["public.users"])):
        a = _cfg(db_type, "cdc-0a1b2c3d", "app", tables)
        b = _cfg(db_type, "cdc-9f8e7d6c", "app", tables)
        for key in IDENTITY_KEYS[db_type]:
            assert a.get(key), f"{db_type}: {key} missing"
            assert a[key] != b[key], f"{db_type}: both pipelines on database app get {key}={a[key]!r}"


def test_one_connection_per_database_on_one_server():
    a = _cfg("mysql", "cdc-11111111", "sales", ["sales.orders"])
    b = _cfg("mysql", "cdc-22222222", "hr", ["hr.people"])
    for key in IDENTITY_KEYS["mysql"]:
        assert a[key] != b[key], f"mysql: sales and hr pipelines share {key}={a[key]!r}"


def test_same_pipeline_redeploys_with_the_same_identity():
    """Control: a redeploy must reuse its server id and slot, not mint new ones."""
    for db_type, tables in (("mysql", ["app.users"]), ("postgresql", ["public.users"])):
        a = _cfg(db_type, "cdc-0a1b2c3d", "app", tables)
        again = _cfg(db_type, "cdc-0a1b2c3d", "app", tables)
        for key in IDENTITY_KEYS[db_type]:
            assert a[key] == again[key], f"{db_type}: {key} changed across redeploys: {a[key]!r} vs {again[key]!r}"


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
