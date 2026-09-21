"""Server-level MongoDB connections: no database named on the connection.

Discovery lists every database the login can see, minus MongoDB's own (admin,
config, local), narrowed by the scope filter. Reads take a
``<database>.<collection>`` name, split at the FIRST dot (a database name cannot
contain one, a collection name can). A connection that names a database keeps
its historical behaviour.

Offline: _mongo_fakes swaps the _get_client seam.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import connector as mg  # noqa: E402
import _mongo_fakes  # noqa: E402

SERVER = {"host": "localhost", "port": 27017, "user": "u", "password": "p"}

DBS = {
    "admin": {"system.users": [{"_id": 1}]},
    "local": {"oplog.rs": [{"_id": 1}]},
    "config": {"chunks": [{"_id": 1}]},
    "shop": {"users": [{"_id": 1, "name": "a"}], "orders": [{"_id": 1, "total": 3}],
             "system.views": []},
    "shop_archive": {"users": [{"_id": 9, "name": "old"}]},
    "test": {"users": [{"_id": 2, "name": "t"}], "logs.2026": [{"_id": 3}]},
}


def _cfg(**extra):
    return {"config": {**SERVER, **extra}}


def _tables(out):
    return [(t["schema"], t["name"]) for t in out["tables"]]


def test_discovery_spans_every_user_database_sorted():
    s, client = _mongo_fakes.make_connector(mg, dbs=DBS)
    out = s.discover_schema(_cfg())
    assert out["overall_status"] == "success", out
    assert _tables(out) == [
        ("shop", "orders"), ("shop", "users"),
        ("shop_archive", "users"),
        ("test", "logs.2026"), ("test", "users"),
    ], _tables(out)
    assert out["total_tables_available"] == 5, out
    assert client.closed


def test_scope_include_and_exclude_with_wildcards():
    s, _ = _mongo_fakes.make_connector(mg, dbs=DBS)
    inc = s.discover_schema(_cfg(namespace_filter_mode="include", namespace_filter_patterns="shop*"))
    assert {db for db, _ in _tables(inc)} == {"shop", "shop_archive"}, _tables(inc)
    exc = s.discover_schema(_cfg(namespace_filter_mode="exclude", namespace_filter_patterns="*archive, TEST"))
    assert {db for db, _ in _tables(exc)} == {"shop"}, _tables(exc)


def test_scope_can_never_reach_system_databases():
    s, _ = _mongo_fakes.make_connector(mg, dbs=DBS)
    out = s.discover_schema(_cfg(namespace_filter_mode="include", namespace_filter_patterns="admin,local,config"))
    assert out["tables"] == [], out
    assert mg.namespace_filter.NO_MATCH_WARNING in out["warnings_messages"], out
    assert out["overall_status"] == "success", "no match is a warning, not a failure"


def test_invalid_scope_fails_closed_before_connecting():
    s, client = _mongo_fakes.make_connector(mg, dbs=DBS)
    out = s.discover_schema(_cfg(namespace_filter_mode="include"))
    assert out["overall_status"] == "failed" and out["tables"] == [], out
    assert any("at least one pattern" in w for w in out["warnings_messages"]), out
    bad = s.validate_config(_cfg(namespace_filter_mode="sometimes"))
    assert bad["valid"] is False and any("must be one of" in e for e in bad["errors"]), bad


def test_named_database_ignores_the_scope_filter():
    s, _ = _mongo_fakes.make_connector(mg, dbs=DBS)
    out = s.discover_schema(_cfg(database="shop", namespace_filter_mode="include",
                                 namespace_filter_patterns="test"))
    assert _tables(out) == [("shop", "orders"), ("shop", "users")], _tables(out)


def test_max_tables_is_shared_across_databases():
    s, _ = _mongo_fakes.make_connector(mg, dbs=DBS)
    out = s.discover_schema({**_cfg(), "max_tables": 2})
    assert _tables(out) == [("shop", "orders"), ("shop", "users")], _tables(out)
    assert out["total_tables_available"] == 5, out


def test_export_reads_the_qualified_database_split_at_the_first_dot():
    s, _ = _mongo_fakes.make_connector(mg, dbs=DBS)
    out = s.export({**_cfg(), "table": "shop_archive.users"})
    assert out["success"] is True and [r["name"] for r in out["data"]] == ["old"], out
    dotted = s.export({**_cfg(), "table": "test.logs.2026"})
    assert dotted["success"] is True and dotted["row_count"] == 1, dotted


def test_export_needs_a_qualified_in_scope_name_on_a_server_level_connection():
    s, _ = _mongo_fakes.make_connector(mg, dbs=DBS)
    bare = s.export({**_cfg(), "table": "users"})
    assert bare["success"] is False and "<database>.<collection>" in bare["error"], bare
    out_of_scope = s.export({**_cfg(namespace_filter_mode="exclude", namespace_filter_patterns="shop"),
                             "table": "shop.users"})
    assert out_of_scope["success"] is False and "outside" in out_of_scope["error"], out_of_scope
    system = s.export({**_cfg(), "table": "admin.system.users"})
    assert system["success"] is False and "outside" in system["error"], system


def test_named_database_export_keeps_the_last_segment():
    s, _ = _mongo_fakes.make_connector(mg, dbs=DBS)
    out = s.export({**_cfg(database="test"), "table": "shop.users"})
    assert out["success"] is True and [r["name"] for r in out["data"]] == ["t"], out


def test_find_on_a_server_level_connection_takes_the_database_param():
    s, _ = _mongo_fakes.make_connector(mg, dbs=DBS)
    out = s.find({**_cfg(), "database": "shop_archive", "collection": "users"})
    assert out["success"] is True and out["returned"] == 1, out
    missing = s.find({**_cfg(), "collection": "users"})
    assert missing["error_code"] == "invalid_database", missing
    for bad in ("admin", "a.b", "a/b", "x" * 65):
        out = s.find({**_cfg(), "database": bad, "collection": "users"})
        assert out["error_code"] == "invalid_database", (bad, out)


def test_find_on_a_named_database_refuses_another_database():
    s, _ = _mongo_fakes.make_connector(mg, dbs=DBS)
    same = s.find({**_cfg(database="shop"), "database": "shop", "collection": "users"})
    assert same["success"] is True, same
    other = s.find({**_cfg(database="shop"), "database": "test", "collection": "users"})
    assert other["error_code"] == "invalid_database", other


def test_writes_without_a_database_or_namespace_say_what_to_set():
    s, client = _mongo_fakes.make_connector(mg, dbs=DBS)
    out = s.import_data({**_cfg(), "table": "users", "data": [{"_id": 5}]})
    assert out["success"] is False and "destination database" in out["error"], out
    ok = s.import_data({**_cfg(), "table": "users", "data": [{"_id": 5}], "namespace": "shop"})
    assert ok["success"] is True, ok
    assert any(d.get("_id") == 5 for d in client["shop"]["users"].docs()), client["shop"]["users"].docs()
