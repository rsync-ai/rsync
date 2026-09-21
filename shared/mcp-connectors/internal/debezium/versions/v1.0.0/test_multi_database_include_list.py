#!/usr/bin/env python3
"""A server-level MySQL/MongoDB connection (no database named) captures tables
from several databases: database.include.list must name every one of them.

Naming only the first database (the old `db_name or first_db`) makes Debezium
filter out every other database's changes before table.include.list is even
consulted, so those tables snapshot nothing and stream nothing.

Run: python3 -m pytest test_multi_database_include_list.py
"""
import connector


def _cfg(db_type, **overrides):
    args = {
        "database_type": db_type,
        "connector_name": f"cdc-{db_type}123",
        "db_host": "db",
        "db_user": "u",
        "db_password": "p",
    }
    args.update(overrides)
    _, cfg, _ = connector.DebeziumConnector()._build_config(args)
    return cfg


def test_mysql_tables_in_several_databases_include_every_database():
    cfg = _cfg("mysql", tables=["shop.orders", "crm.contacts", "shop.users"])
    assert cfg["database.include.list"] == "shop,crm"
    assert cfg["table.include.list"] == "shop.orders,crm.contacts,shop.users"


def test_mysql_named_database_is_unchanged():
    cfg = _cfg("mysql", db_name="shop", tables=["orders", "users"])
    assert cfg["database.include.list"] == "shop"
    assert cfg["table.include.list"] == "shop.orders,shop.users"


def test_mysql_one_qualified_database():
    cfg = _cfg("mysql", tables=["shop.orders"])
    assert cfg["database.include.list"] == "shop"


def test_mongo_collections_in_several_databases_include_every_database():
    cfg = _cfg("mongodb", tables=["shop.orders", "crm.contacts", "shop.order.lines"])
    assert cfg["database.include.list"] == "shop,crm"
    assert cfg["collection.include.list"] == "shop.orders,crm.contacts,shop.order.lines"
    assert "capture.scope" not in cfg  # several databases: deployment-wide stream


def test_mongo_named_database_is_unchanged():
    cfg = _cfg("mongodb", db_name="shop", tables=["orders"])
    assert cfg["database.include.list"] == "shop"
    assert cfg["capture.target"] == "shop"


def test_capture_database_helpers_refuse_to_guess():
    assert connector.mysql_capture_databases("shop.a,b") == []
    assert connector.mysql_capture_databases("") == []
    assert connector.mysql_capture_databases("a.x, b.y ,a.z") == ["a", "b"]
