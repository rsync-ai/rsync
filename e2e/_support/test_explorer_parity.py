"""
Unit tests for the explorer_parity helper.

These exercise the helper's *logic* without a real backend or DB: the
Explorer HTTP call is mocked at the `requests` layer, and the docker-exec
side is mocked at `subprocess.check_output`. They cover the failure modes
the helper is meant to catch (count/column mismatches), the happy path,
and the small bits of glue (alias-named COUNT(*), MySQL dialect SQL).

Runnable as a CI unit test — no Docker, no api-gateway, no Postgres.
"""

from __future__ import annotations

from typing import Any

import pytest

from e2e._support import explorer_parity as ep
from e2e._support.explorer_parity import DBHandle, assert_explorer_matches_db


# --------------------------------------------------------------------------- #
# Fakes
# --------------------------------------------------------------------------- #


class _FakeResponse:
    def __init__(self, status: int, body: dict[str, Any]) -> None:
        self.status_code = status
        self.ok = 200 <= status < 300
        self._body = body
        self.text = str(body)

    def json(self) -> dict[str, Any]:
        return self._body


def _patch_explorer(monkeypatch, queue: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Patch requests.post so the helper consumes responses from ``queue``.

    Returns a list of captured request payloads for assertion.
    """
    captured: list[dict[str, Any]] = []

    def fake_post(url: str, *, json: dict[str, Any], cookies: dict[str, str], timeout: float):
        captured.append({"url": url, "json": json, "cookies": cookies})
        if not queue:
            raise AssertionError(f"unexpected Explorer call: {json}")
        return _FakeResponse(200, queue.pop(0))

    monkeypatch.setattr(ep.requests, "post", fake_post)
    return captured


def _patch_subprocess(monkeypatch, outputs: dict[str, str]) -> list[list[str]]:
    """Patch subprocess.check_output to return canned text per SQL keyword.

    ``outputs`` keys are substrings matched against the command's SQL arg.
    """
    captured: list[list[str]] = []

    def fake_check_output(cmd: list[str], *, text: bool) -> str:
        captured.append(cmd)
        sql = cmd[-1]
        for key, value in outputs.items():
            if key in sql:
                return value
        raise AssertionError(f"no canned output matches SQL: {sql!r}")

    monkeypatch.setattr(ep.subprocess, "check_output", fake_check_output)
    return captured


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #


PG = DBHandle(container="rsync-pg", user="e2e_user", database="e2e_db", dialect="postgres")
MY = DBHandle(container="rsync-mysql", user="root", database="e2e_db", dialect="mysql")


def test_dbhandle_rejects_bad_dialect():
    with pytest.raises(ValueError, match="unsupported dialect"):
        DBHandle(container="c", user="u", database="d", dialect="oracle")


def test_happy_path_postgres(monkeypatch):
    explorer_calls = _patch_explorer(
        monkeypatch,
        queue=[
            # 1. COUNT(*) — alias "c"
            {"columns": ["c"], "rows": [{"c": 3}], "row_count": 1, "execution_time_ms": 1, "truncated": False},
            # 2. SELECT * LIMIT 1
            {
                "columns": ["id", "amount", "created_at"],
                "rows": [{"id": 1, "amount": "12.34", "created_at": "2026-02-02T10:11:12"}],
                "row_count": 1,
                "execution_time_ms": 1,
                "truncated": False,
            },
        ],
    )
    _patch_subprocess(
        monkeypatch,
        outputs={
            "COUNT(*)": "3",
            "information_schema.columns": "id\namount\ncreated_at",
        },
    )

    assert_explorer_matches_db(
        api_gateway_url="http://gw",
        auth_token="tok",
        connection_id="conn-1",
        db=PG,
        table="payments",
    )

    assert len(explorer_calls) == 2
    assert explorer_calls[0]["json"]["sql"].startswith("SELECT COUNT(*)")
    assert explorer_calls[1]["json"]["sql"].startswith("SELECT * FROM")


def test_count_mismatch_fails(monkeypatch):
    _patch_explorer(
        monkeypatch,
        queue=[
            {"columns": ["c"], "rows": [{"c": 5}], "row_count": 1, "execution_time_ms": 1, "truncated": False},
        ],
    )
    _patch_subprocess(monkeypatch, outputs={"COUNT(*)": "3"})

    with pytest.raises(AssertionError, match=r"row count mismatch on public\.payments: explorer=5 db=3"):
        assert_explorer_matches_db(
            api_gateway_url="http://gw",
            auth_token="tok",
            connection_id="conn-1",
            db=PG,
            table="payments",
        )


def test_column_mismatch_fails(monkeypatch):
    _patch_explorer(
        monkeypatch,
        queue=[
            {"columns": ["c"], "rows": [{"c": 0}], "row_count": 1, "execution_time_ms": 1, "truncated": False},
            # Explorer is missing "created_at" that the DB has — schema drift bug.
            {"columns": ["id", "amount"], "rows": [], "row_count": 0, "execution_time_ms": 1, "truncated": False},
        ],
    )
    _patch_subprocess(
        monkeypatch,
        outputs={
            "COUNT(*)": "0",
            "information_schema.columns": "id\namount\ncreated_at",
        },
    )

    with pytest.raises(AssertionError, match=r"column-set mismatch.*only_in_db=\['created_at'\]"):
        assert_explorer_matches_db(
            api_gateway_url="http://gw",
            auth_token="tok",
            connection_id="conn-1",
            db=PG,
            table="payments",
        )


def test_mysql_uses_backticks_and_database_schema(monkeypatch):
    _patch_explorer(
        monkeypatch,
        queue=[
            {"columns": ["c"], "rows": [{"c": 1}], "row_count": 1, "execution_time_ms": 1, "truncated": False},
            {"columns": ["id"], "rows": [{"id": 1}], "row_count": 1, "execution_time_ms": 1, "truncated": False},
        ],
    )
    captured = _patch_subprocess(
        monkeypatch,
        outputs={
            "COUNT(*)": "1",
            "information_schema.columns": "id",
        },
    )

    assert_explorer_matches_db(
        api_gateway_url="http://gw",
        auth_token="tok",
        connection_id="conn-1",
        db=MY,
        table="orders",
    )

    count_cmd = captured[0]
    assert count_cmd[0:2] == ["docker", "exec"]
    assert "mysql" in count_cmd
    assert "`orders`" in count_cmd[-1]
    info_schema_cmd = captured[1]
    assert "table_schema='e2e_db'" in info_schema_cmd[-1]


def test_explorer_non_2xx_surfaces_body(monkeypatch):
    def fake_post(*_a, **_kw):
        return _FakeResponse(403, {"error": "forbidden"})

    monkeypatch.setattr(ep.requests, "post", fake_post)

    with pytest.raises(AssertionError, match="Explorer query failed.*403.*forbidden"):
        assert_explorer_matches_db(
            api_gateway_url="http://gw",
            auth_token="tok",
            connection_id="conn-1",
            db=PG,
            table="payments",
        )
