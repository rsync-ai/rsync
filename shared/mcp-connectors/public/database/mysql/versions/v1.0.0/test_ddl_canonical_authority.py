"""Regression lock: the mysql sink's _ddl_for delegates canonical->DDL to the
shared canonical_to_ddl("mysql", ...) authority (canonical_types.py).

Companion to the postgresql sink's test_decimal_scale_preservation
``test_pg_type_for_delegates_to_shared_authority``. Two MySQL-sink-specific
concerns stay LOCAL, ahead of the delegation, and are pinned here:

  1. CDC verbatim passthrough — kafka-mcp-sink emits already-resolved MySQL DDL
     in the streaming path (CHAR(36)/DECIMAL(12,4)/TINYINT(1)/VARBINARY(255));
     tokens whose base is in ``_MYSQL_DDL_BASES`` are preserved verbatim so the
     parametric width/scale survives. This also means canonical names that
     collide with a MySQL keyword (``integer``, ``binary``, ``date``, ``time``,
     ``json``) keep their passthrough form (INTEGER/BINARY/DATE/...) exactly as
     the pre-refactor connector did — behavior preserved, not changed here.
  2. ``uuid`` -> CHAR(36): MySQL has no native UUID; the shared canonical vocab
     folds uuid -> string (would widen to TEXT), so the sink keeps CHAR(36) (the
     same guard pattern as the PG sink's uuid -> UUID).

Everything else routes to the shared authority. The migration is
behavior-preserving except for source-dialect tokens the old local map silently
dumped to TEXT (money/double precision/serial/int2../bit varying/datetime2/...),
which now map to their correct MySQL type — a strict improvement, verified by a
73-identical / 13-TEXT->correct equivalence sweep.

Run inside the mysql connector image (deps available):
    docker cp <thisfile> rsync-ai-mysql-v1-0-0-mcp:/app/ && \
    docker exec rsync-ai-mysql-v1-0-0-mcp python -m pytest /app/test_ddl_canonical_authority.py -q
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from connector import MysqlMCPServer  # noqa: E402


class _RecordingCursor:
    """DB-API cursor stub: records SQL, reports the table as not-yet-existing so
    we observe the CREATE TABLE DDL _ddl_for produced."""

    def __init__(self):
        self.sql = []

    def execute(self, query, params=None):
        self.sql.append(query)

    def fetchall(self):
        return []

    def fetchone(self):
        return None

    def close(self):
        pass


def _col_ddl(col_type) -> str:
    """Exact DDL type _ddl_for emits for a single ``amount`` column."""
    srv = MysqlMCPServer.__new__(MysqlMCPServer)  # bypass __init__ (no DB/env)
    cur = _RecordingCursor()
    srv._ensure_table_for_cdc(
        cur, {"database": "testdb"}, "t_ddl", ["amount"], [],
        {"amount": col_type}, {},
    )
    create = next(s for s in cur.sql if s.strip().upper().startswith("CREATE TABLE"))
    inner = create[create.index("(") + 1: create.rindex(")")]
    return inner.split("`amount`", 1)[1].strip()


def test_canonical_vocabulary_maps_to_mysql_ddl():
    # Canonical names that do NOT collide with a MySQL keyword go through the
    # shared authority.
    assert _col_ddl("string") == "TEXT"
    assert _col_ddl("number") == "DOUBLE"
    assert _col_ddl("boolean") == "TINYINT(1)"
    assert _col_ddl("timestamp") == "DATETIME(6)"
    # Bare decimal -> shared safe width (never MySQL's truncating DECIMAL(10,0)).
    assert _col_ddl("decimal") == "DECIMAL(38,9)"


def test_keyword_colliding_canonicals_keep_passthrough_form():
    # These canonical names are ALSO in _MYSQL_DDL_BASES, so the CDC passthrough
    # intercepts them first — unchanged from the pre-refactor connector.
    assert _col_ddl("integer") == "INTEGER"
    assert _col_ddl("binary") == "BINARY"
    assert _col_ddl("date") == "DATE"
    assert _col_ddl("time") == "TIME"
    assert _col_ddl("json") == "JSON"


def test_cdc_passthrough_preserves_parametric_ddl():
    # Already-resolved MySQL DDL from the streaming sink survives verbatim.
    assert _col_ddl("CHAR(36)") == "CHAR(36)"
    assert _col_ddl("DECIMAL(12,4)") == "DECIMAL(12,4)"
    assert _col_ddl("numeric(10,2)") == "numeric(10,2)"
    assert _col_ddl("TINYINT(1)") == "TINYINT(1)"
    assert _col_ddl("VARBINARY(255)") == "VARBINARY(255)"


def test_uuid_guard_keeps_char36():
    # Shared vocab folds uuid -> string (-> TEXT); local guard keeps CHAR(36).
    assert _col_ddl("uuid") == "CHAR(36)"


def test_stray_source_dialect_tokens_fold_via_shared_authority():
    # Tokens the OLD local map silently dumped to TEXT now map correctly.
    assert _col_ddl("money") == "DECIMAL(38,9)"
    assert _col_ddl("double precision") == "DOUBLE"
    assert _col_ddl("int8") == "BIGINT"
    assert _col_ddl("serial") == "BIGINT"
    assert _col_ddl("bit varying") == "BLOB"
    assert _col_ddl("datetime2") == "DATETIME(6)"     # SS datetime alias
    assert _col_ddl("timestamptz") == "DATETIME(6)"   # PG tz timestamp
    assert _col_ddl("jsonb") == "JSON"
    assert _col_ddl("bytea") == "BLOB"


def test_unknown_type_falls_back_to_text():
    # Never fail CREATE TABLE on an unrecognized scalar.
    assert _col_ddl("some_unknown_xyz") == "TEXT"


if __name__ == "__main__":
    test_canonical_vocabulary_maps_to_mysql_ddl()
    test_keyword_colliding_canonicals_keep_passthrough_form()
    test_cdc_passthrough_preserves_parametric_ddl()
    test_uuid_guard_keeps_char36()
    test_stray_source_dialect_tokens_fold_via_shared_authority()
    test_unknown_type_falls_back_to_text()
    print("OK: mysql _ddl_for canonical-authority delegation tests passed")
