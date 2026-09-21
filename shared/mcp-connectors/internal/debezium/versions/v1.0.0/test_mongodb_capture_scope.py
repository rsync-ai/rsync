#!/usr/bin/env python3
"""MongoDB change streams must be scoped to the database rsync captures from (#19).

Debezium 3.x defaults to capture.scope=deployment, a cluster-wide change stream
that needs changeStream+find on EVERY database. A user granted read on only the
source database -- the normal Atlas setup -- is refused, Debezium retries without
limit, and Kafka Connect keeps reporting the task RUNNING. The pipeline shows
Running and writes nothing. Probed on Debezium 3.1: the same read-on-one-database
user produced no topic under the default scope and the full snapshot plus live
inserts under capture.scope=database.

Run: python3 -m pytest test_mongodb_capture_scope.py
"""
import connector


def _mongo_cfg(**overrides):
    args = {
        "database_type": "mongodb",
        "connector_name": "cdc-mongo123",
        "db_host": "mongo",
        "db_user": "u",
        "db_password": "p",
    }
    args.update(overrides)
    _, cfg, _ = connector.DebeziumConnector()._build_config(args)
    return cfg


def test_single_database_from_db_name_scopes_the_change_stream():
    cfg = _mongo_cfg(db_name="shop", tables=["orders", "customers"])
    assert cfg["collection.include.list"] == "shop.orders,shop.customers"
    assert cfg["capture.scope"] == "database"
    assert cfg["capture.target"] == "shop"


def test_single_database_from_qualified_collections_scopes_the_change_stream():
    # No db_name: the database comes from the qualified entries themselves.
    cfg = _mongo_cfg(tables=["shop.orders", "shop.order.lines"])
    assert cfg["capture.scope"] == "database"
    assert cfg["capture.target"] == "shop"


def test_atlas_uri_single_database_is_scoped():
    cfg = _mongo_cfg(
        db_host="",
        db_user="",
        connection_string="mongodb+srv://svc:pw@cluster0.abcd.mongodb.net/?retryWrites=true",
        db_name="shop",
        tables=["orders"],
    )
    assert cfg["capture.scope"] == "database"
    assert cfg["capture.target"] == "shop"


def test_collections_in_several_databases_keep_the_deployment_default():
    # One database-scoped stream cannot see two databases; guessing one would
    # silently drop the other's changes.
    cfg = _mongo_cfg(tables=["shop.orders", "crm.contacts"])
    assert "capture.scope" not in cfg
    assert "capture.target" not in cfg


def test_unqualified_collection_without_a_database_is_not_scoped():
    cfg = _mongo_cfg(tables=["orders"])
    assert "capture.scope" not in cfg
    assert "capture.target" not in cfg


def test_mongo_capture_databases():
    f = connector.mongo_capture_databases
    assert f("shop.orders") == ["shop"]
    assert f("shop.orders, shop.customers") == ["shop"]
    assert f("shop.orders,crm.contacts") == ["shop", "crm"]
    assert f("shop.orders.archive") == ["shop"]  # dots belong to the collection
    assert f("orders") == []
    assert f("shop.orders,orders") == []  # one unqualified entry: don't guess
    assert f(r"shop\.orders") == []  # a regex, not a database name
    assert f("sh$op.orders") == []
    assert f(".orders") == []
    assert f("shop.") == []
    assert f("") == []


def test_relational_sources_never_get_mongo_capture_keys():
    _, cfg, _ = connector.DebeziumConnector()._build_config(
        {
            "database_type": "postgresql",
            "connector_name": "cdc-pg123",
            "db_host": "pg",
            "db_user": "u",
            "db_password": "p",
            "db_name": "app",
            "tables": ["public.users"],
        }
    )
    assert "capture.scope" not in cfg
    assert "capture.target" not in cfg
