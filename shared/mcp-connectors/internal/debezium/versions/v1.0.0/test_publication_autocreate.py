#!/usr/bin/env python3
"""Tests for the PostgreSQL publication ordering invariant.

The publication MUST exist before the replication slot. The orchestrator
provisions them in that order (backend-orchestrator/internal/cdc/postgresql.go)
and then tells Debezium to keep its hands off. If Debezium creates the
publication itself instead, pgoutput decodes the first WAL batch without knowing
which tables are included: the rows are gone and nothing anywhere logs an error.

That makes the DEFAULT the thing worth testing — a caller that forgets to pass
publication_autocreate_mode must fail loudly (publication does not exist), never
lose rows quietly.

Run: python3 test_publication_autocreate.py
"""
import os

import connector


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


def test_default_is_disabled_not_filtered():
    """The caller that forgets the argument is the one this protects."""
    assert _pg_config()["publication.autocreate.mode"] == "disabled"


def test_caller_can_still_pass_the_mode_explicitly():
    """The orchestrator passes "disabled" today; the argument stays honoured so
    a deliberate choice is still possible (and still visible in the diff)."""
    cfg = _pg_config(publication_autocreate_mode="disabled")
    assert cfg["publication.autocreate.mode"] == "disabled"


def test_mysql_gets_no_publication_key():
    """publication.autocreate.mode is PostgreSQL-only; Connect rejects an
    unknown property outright."""
    _, cfg, _ = connector.DebeziumConnector()._build_config(
        {
            "database_type": "mysql",
            "connector_name": "cdc-mysql123",
            "db_host": "db",
            "db_user": "svc",
            "db_password": "pw",
            "db_name": "app",
            "tables": ["app.users"],
        }
    )
    assert "publication.autocreate.mode" not in cfg


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
