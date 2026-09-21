"""list_namespaces on MongoDB lists the databases, without MongoDB's own
(admin, config, local), and reports the database the connection names as
"current".

Offline: _mongo_fakes swaps the _get_client seam; no replica set needed.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import connector as mg  # noqa: E402
import _mongo_fakes  # noqa: E402

CFG = {"config": {"host": "localhost", "port": 27017, "database": "appdb",
                  "user": "u", "password": "p"}}


def test_lists_user_databases_and_closes():
    s, client = _mongo_fakes.make_connector(
        mg, dbs={"admin": {}, "local": {}, "config": {}, "appdb": {}, "billing": {}})
    out = s.list_namespaces(CFG)
    assert out == {"success": True, "namespaces": ["appdb", "billing"], "current": "appdb"}, out
    assert client.closed


def test_no_database_in_config_means_no_current():
    s, _ = _mongo_fakes.make_connector(mg, dbs={"appdb": {}})
    cfg = {"config": {k: v for k, v in CFG["config"].items() if k != "database"}}
    out = s.list_namespaces(cfg)
    assert out["success"] is True and out["current"] == "", out


def test_failure_is_an_error():
    s, client = _mongo_fakes.make_connector(mg)

    def boom():
        raise RuntimeError("not authorized on admin")

    client.list_database_names = boom
    out = s.list_namespaces(CFG)
    assert out["success"] is False and "not authorized" in out["error"], out
    assert client.closed
