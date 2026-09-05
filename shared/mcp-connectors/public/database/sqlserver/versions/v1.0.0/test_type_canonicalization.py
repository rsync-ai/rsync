"""Regression test: SQL Server discovery emits CANONICAL column types (not raw
T-SQL names) and preserves decimal precision/scale.

Before this fix, `_discover_sqlserver_schema_v2` emitted the raw `sys.types.name`
(`datetime2`, `nvarchar`, bare `decimal`, ...). Downstream destinations map a
canonical vocabulary, so raw names fell through: `datetime2` -> dest TEXT (lost
the timestamp type) and bare `decimal` -> dest NUMERIC(38,9) (lost the source's
precision/scale). Now discovery emits canonical (`integer`/`number`/`decimal`/
`decimal(p,s)`/`string`/`boolean`/`timestamp`/`date`/`time`/`binary`) like the
PostgreSQL connector, so every destination maps a single vocabulary.

Offline: a fake cursor answers the discovery query sequence by SQL shape + bind
params; no real SQL Server needed.

Run inside the sqlserver connector image:
    docker cp <thisfile> rsync-ai-sqlserver-v1-0-0-mcp:/app/ && \
    docker exec rsync-ai-sqlserver-v1-0-0-mcp python /app/test_type_canonicalization.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from connector import MysqlMCPServer, _canonical_sqlserver_type  # noqa: E402


# ---- unit tests for the pure canonicalizer -------------------------------

def test_canonical_map_covers_sqlserver_types():
    cases = {
        # integers
        "int": "integer", "bigint": "integer", "smallint": "integer", "tinyint": "integer",
        # approximate + exact numerics
        "real": "number", "float": "number",
        "decimal": "decimal", "numeric": "decimal", "money": "decimal", "smallmoney": "decimal",
        # bit is boolean in SQL Server
        "bit": "boolean",
        # strings (incl. unicode / xml / guid)
        "char": "string", "varchar": "string", "nchar": "string", "nvarchar": "string",
        "text": "string", "ntext": "string", "xml": "string", "uniqueidentifier": "string",
        # date/time — the datetime2 family was previously unmapped -> TEXT
        "date": "date", "time": "time",
        "datetime": "timestamp", "datetime2": "timestamp",
        "smalldatetime": "timestamp", "datetimeoffset": "timestamp",
        # binary — SQL Server timestamp/rowversion is an 8-byte row version, NOT a datetime
        "binary": "binary", "varbinary": "binary", "image": "binary",
        "rowversion": "binary", "timestamp": "binary",
    }
    for raw, expected in cases.items():
        assert _canonical_sqlserver_type(raw) == expected, f"{raw} -> {_canonical_sqlserver_type(raw)} != {expected}"
    # case-insensitive + precision-suffix stripped + unknown -> string
    assert _canonical_sqlserver_type("DATETIME2(7)") == "timestamp"
    assert _canonical_sqlserver_type("DECIMAL(12,2)") == "decimal"
    assert _canonical_sqlserver_type("hierarchyid") == "string"  # unknown -> string
    assert _canonical_sqlserver_type(None) == "string"


# ---- discovery-emission test (raw sys.types -> canonical col types) --------

# (name, sys.types.name, is_nullable_bit, precision, scale)
_COLS = [
    ("id",          "int",              0, 10, 0),
    ("big",         "bigint",           0, 19, 0),
    ("amount",      "decimal",          0, 12, 2),   # precision preserved
    ("price",       "numeric",          1, 18, 4),
    ("cash",        "money",            1, 19, 4),
    ("is_active",   "bit",              0,  1, 0),
    ("name",        "nvarchar",         1,  0, 0),
    ("descr",       "varchar",          1,  0, 0),
    ("doc",         "xml",              1,  0, 0),
    ("guid",        "uniqueidentifier", 0,  0, 0),
    ("created_at",  "datetime2",        0, 27, 7),   # was TEXT before the fix
    ("created_off", "datetimeoffset",   1, 34, 7),
    ("legacy_dt",   "smalldatetime",    1, 16, 0),
    ("plain_dt",    "datetime",         1, 23, 3),
    ("d",           "date",             1, 10, 0),
    ("t",           "time",             1, 16, 7),
    ("rv",          "rowversion",       0,  0, 0),   # 8-byte row version -> binary
    ("ts",          "timestamp",        0,  0, 0),   # SS alias of rowversion -> binary
    ("blob",        "varbinary",        1,  0, 0),
]

_EXPECTED = {
    "id": "integer", "big": "integer",
    "amount": "decimal(12,2)", "price": "decimal(18,4)", "cash": "decimal(19,4)",
    "is_active": "boolean",
    "name": "string", "descr": "string", "doc": "string", "guid": "string",
    "created_at": "timestamp", "created_off": "timestamp",
    "legacy_dt": "timestamp", "plain_dt": "timestamp",
    "d": "date", "t": "time",
    "rv": "binary", "ts": "binary", "blob": "binary",
}


class FakeCursor:
    def __init__(self):
        self._result = []
        self._one = None

    def execute(self, sql, params=None):
        s = " ".join(sql.split())
        if "FROM sys.schemas" in s and "NOT IN" in s and "sys.tables" not in s:
            self._result = [("dbo",)]
        elif "COUNT(*)" in s and "sys.tables" in s:
            self._one = [1]
        elif "index_id IN (0,1)" in s:
            self._result = [("typed", 1)]
        elif "sys.columns c" in s and "sys.types" in s:
            self._result = list(_COLS)
        else:
            self._result = []

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._result

    def close(self):
        pass


def _discover():
    srv = MysqlMCPServer.__new__(MysqlMCPServer)
    cur = FakeCursor()
    srv._get_cursor = lambda conn, as_dict=False: cur
    result = {}
    srv._discover_sqlserver_schema_v2(
        conn=object(), config={}, max_tables=100,
        include_columns=True, include_row_counts=False,
        include_relationships=False, include_indexes=False,
        result=result, add_warning=lambda *a, **k: None,
    )
    return result


def test_discovery_emits_canonical_types_with_precision():
    result = _discover()
    tbl = next(t for t in result["tables"] if t["name"] == "typed")
    got = {c["name"]: c["type"] for c in tbl["columns"]}
    for name, expected in _EXPECTED.items():
        assert got.get(name) == expected, f"{name}: {got.get(name)} != {expected}"
    # raw T-SQL name preserved for observability
    src = {c["name"]: c.get("source_type") for c in tbl["columns"]}
    assert src["created_at"] == "datetime2"
    assert src["amount"] == "decimal"


def test_datetime2_no_longer_text_and_decimal_keeps_scale():
    result = _discover()
    tbl = next(t for t in result["tables"] if t["name"] == "typed")
    got = {c["name"]: c["type"] for c in tbl["columns"]}
    assert got["created_at"] == "timestamp"        # was would-be TEXT
    assert got["amount"] == "decimal(12,2)"        # was would-be NUMERIC(38,9)


if __name__ == "__main__":
    test_canonical_map_covers_sqlserver_types()
    test_discovery_emits_canonical_types_with_precision()
    test_datetime2_no_longer_text_and_decimal_keeps_scale()
    print("OK: sqlserver type-canonicalization tests passed")
