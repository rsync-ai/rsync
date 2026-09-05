"""
Reusable parity check between the Data Explorer view and the destination DB.

Used by E2E tests after a pipeline run to assert that what the Explorer
reports for a given destination table matches what's actually in the
destination database (api-gateway -> destination DB direct).

Two independent checks per table:

  1. **Row count parity** — ``COUNT(*)`` via Explorer query equals
     ``COUNT(*)`` via direct DB query.
  2. **Column-set parity** — column names returned by Explorer equal
     ``information_schema.columns`` for the table.

Why split into separate checks? Each fails for a distinct class of bug:
- count mismatch -> sink dropped/duplicated rows;
- column mismatch -> schema-drift handler missed a column.

Value-level content checksumming is **intentionally not** in this helper
yet: Explorer returns rows as JSON (driver-typed values) while psql
returns raw CSV, and reconciling those representations without false
positives is its own piece of work. Tracked as TODO(qa-p1-04b).

The helper does not depend on a Python DB driver. To match existing
e2e conventions, direct DB reads are done via ``docker exec`` against
the canonical ``psql`` / ``mysql`` CLIs.
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from typing import Any

import requests


@dataclass(frozen=True)
class DBHandle:
    """Coordinates for shelling into a destination DB via docker exec."""

    container: str
    user: str
    database: str
    dialect: str  # "postgres" or "mysql"

    def __post_init__(self) -> None:
        if self.dialect not in {"postgres", "mysql"}:
            raise ValueError(f"unsupported dialect: {self.dialect!r}")


# --------------------------------------------------------------------------- #
# Explorer side
# --------------------------------------------------------------------------- #


def _explorer_query(
    *,
    api_gateway_url: str,
    auth_token: str,
    connection_id: str,
    sql: str,
    limit: int,
    timeout_s: float = 30.0,
) -> dict[str, Any]:
    """POST /api/v1/explorer/query and return the parsed JSON body.

    Raises AssertionError with the response body on non-2xx, so test
    failures stay readable in pytest output.
    """
    url = f"{api_gateway_url.rstrip('/')}/api/v1/explorer/query"
    resp = requests.post(
        url,
        json={"connection_id": connection_id, "sql": sql, "limit": limit},
        cookies={"auth_token": auth_token},
        timeout=timeout_s,
    )
    assert resp.ok, (
        f"Explorer query failed ({resp.status_code}) for sql={sql!r}: {resp.text}"
    )
    return resp.json()


# --------------------------------------------------------------------------- #
# Direct DB side (docker exec)
# --------------------------------------------------------------------------- #


def _direct_query_csv(db: DBHandle, sql: str) -> str:
    """Run ``sql`` against the destination via docker exec and return CSV.

    Output is one row per line, comma-separated, no header. Matches the
    ``-t -A -F,`` mode that existing tests already use for psql, and a
    ``--batch --raw`` equivalent for mysql.
    """
    if db.dialect == "postgres":
        cmd = [
            "docker", "exec", "-i", db.container,
            "psql", "-U", db.user, "-d", db.database,
            "-t", "-A", "-F", ",", "-c", sql,
        ]
    else:  # mysql
        cmd = [
            "docker", "exec", "-i", db.container,
            "mysql", "-u", db.user, db.database,
            "--batch", "--skip-column-names", "--raw", "-e", sql,
        ]
    return subprocess.check_output(cmd, text=True).strip()


def _direct_count(db: DBHandle, schema: str, table: str) -> int:
    qualified = (
        f'"{schema}"."{table}"' if db.dialect == "postgres" else f"`{table}`"
    )
    out = _direct_query_csv(db, f"SELECT COUNT(*) FROM {qualified};")
    return int(out.splitlines()[0]) if out else 0


def _direct_columns(db: DBHandle, schema: str, table: str) -> list[str]:
    if db.dialect == "postgres":
        sql = (
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_schema='{schema}' AND table_name='{table}' "
            "ORDER BY ordinal_position;"
        )
    else:
        sql = (
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_schema='{db.database}' AND table_name='{table}' "
            "ORDER BY ordinal_position;"
        )
    return [c for c in _direct_query_csv(db, sql).splitlines() if c]


# --------------------------------------------------------------------------- #
# Public helper
# --------------------------------------------------------------------------- #


def assert_explorer_matches_db(
    *,
    api_gateway_url: str,
    auth_token: str,
    connection_id: str,
    db: DBHandle,
    table: str,
    schema: str = "public",
) -> None:
    """Assert Explorer view of ``table`` matches the destination DB.

    Args:
        api_gateway_url: base URL of api-gateway, e.g. ``http://localhost:5001``
        auth_token: value of the ``auth_token`` cookie for an authenticated
            session.
        connection_id: the rsync ``connections.id`` for the destination.
        db: docker-exec coordinates for the same destination.
        table: unqualified table name.
        schema: schema name (Postgres) or unused (MySQL — implied by db).

    Raises:
        AssertionError: with a precise, single-line reason on any mismatch.
    """
    qualified = (
        f'"{schema}"."{table}"' if db.dialect == "postgres" else f"`{table}`"
    )

    # 1. Row counts.
    exp_count_resp = _explorer_query(
        api_gateway_url=api_gateway_url,
        auth_token=auth_token,
        connection_id=connection_id,
        sql=f"SELECT COUNT(*) AS c FROM {qualified}",
        limit=1,
    )
    exp_rows = exp_count_resp.get("rows") or []
    assert exp_rows, f"Explorer returned no rows for COUNT(*) on {table}"
    # COUNT(*) result column might be named "c" (alias) or default-named —
    # take the single column's value regardless.
    exp_count = int(next(iter(exp_rows[0].values())))
    db_count = _direct_count(db, schema, table)
    assert exp_count == db_count, (
        f"row count mismatch on {schema}.{table}: explorer={exp_count} db={db_count}"
    )

    # 2. Column-set parity. We ask Explorer for one row to read its column
    # list; a LIMIT 1 SELECT is cheap on every supported destination.
    exp_sample = _explorer_query(
        api_gateway_url=api_gateway_url,
        auth_token=auth_token,
        connection_id=connection_id,
        sql=f"SELECT * FROM {qualified} LIMIT 1",
        limit=1,
    )
    exp_columns = set(exp_sample.get("columns") or [])
    db_columns = set(_direct_columns(db, schema, table))
    assert exp_columns == db_columns, (
        f"column-set mismatch on {schema}.{table}: "
        f"explorer={sorted(exp_columns)} db={sorted(db_columns)} "
        f"only_in_explorer={sorted(exp_columns - db_columns)} "
        f"only_in_db={sorted(db_columns - exp_columns)}"
    )
