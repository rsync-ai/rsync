"""Regression test: SQL Server discovery enumerates ALL user schemas (not just
`dbo`) and keys columns/PKs by (schema, table) so same-named tables in
different schemas never cross-assign.

Before this fix, `_discover_sqlserver_schema_v2` scanned only
`config.get("schema", "dbo")`. A source connection that omitted `schema`
never discovered tables in other schemas (e.g. `sales.orders`), so no column
types or primary keys were threaded to the destination -> every destination
fell back to all-String columns + a synthetic row-hash PK. (Same class as the
PostgreSQL bug fixed 2026-07-14; mirrors its test_multi_schema_discovery.py.)

Offline: a fake cursor answers the discovery queries by SQL shape + bind
params; no real SQL Server needed.

Run inside the sqlserver connector image:
    docker cp <thisfile> rsync-ai-sqlserver-v1-0-0-mcp:/app/ && \
    docker exec rsync-ai-sqlserver-v1-0-0-mcp python /app/test_multi_schema_discovery.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from connector import MysqlMCPServer  # noqa: E402  (sqlserver reuses this class)


def _srv():
    return MysqlMCPServer.__new__(MysqlMCPServer)  # bypass __init__


# (schema, table) -> columns as (name, sqlserver_type, is_nullable_bit, precision, scale)
_COLUMNS = {
    ("dbo", "orders"): [("id", "int", 0, 10, 0), ("label", "nvarchar", 1, 0, 0)],
    ("dbo", "users"): [("uid", "bigint", 0, 19, 0)],
    ("sales", "orders"): [("id", "bigint", 0, 19, 0), ("amount", "decimal", 0, 12, 2)],
    ("sales", "inventory"): [("sku", "nvarchar", 0, 0, 0)],
}
# (schema, table) -> primary key columns (as fetchall rows)
_PKS = {
    ("dbo", "orders"): [("id",)],
    ("dbo", "users"): [("uid",)],
    ("sales", "orders"): [("id",)],
    ("sales", "inventory"): [("sku",)],
}
# (schema, table) -> estimated row count
_TABLES = {
    "dbo": [("orders", 10), ("users", 5)],
    "sales": [("orders", 20), ("inventory", 7)],
}


class FakeCursor:
    """Answers the discovery query sequence purely from SQL shape + bind params."""

    def __init__(self):
        self._result = []
        self._one = None
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        p = tuple(params or ())
        s = " ".join(sql.split())
        if "FROM sys.schemas" in s and "NOT IN" in s and "sys.tables" not in s:
            # user-schema enumeration
            self._result = [("dbo",), ("sales",)]
        elif "COUNT(*)" in s and "sys.tables" in s:
            sch = p[0] if p else None
            self._one = [len(_TABLES.get(sch, []))]
        elif "index_id IN (0,1)" in s:
            # per-schema table list
            sch = p[0] if p else None
            self._result = list(_TABLES.get(sch, []))
        elif "sys.columns c" in s and "sys.types" in s:
            key = (p[0], p[1]) if len(p) >= 2 else None
            self._result = list(_COLUMNS.get(key, []))
        elif "sys.key_constraints" in s:
            key = (p[0], p[1]) if len(p) >= 2 else None
            self._result = list(_PKS.get(key, []))
        elif "sys.foreign_keys" in s:
            self._result = []
        elif "sys.indexes" in s:
            self._result = []
        else:
            self._result = []

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._result

    def close(self):
        pass


def _discover(config):
    srv = _srv()
    cur = FakeCursor()
    srv._get_cursor = lambda conn, as_dict=False: cur
    result = {}
    srv._discover_sqlserver_schema_v2(
        conn=object(), config=config, max_tables=100,
        include_columns=True, include_row_counts=True,
        include_relationships=True, include_indexes=False,
        result=result, add_warning=lambda *a, **k: None,
    )
    return result


def test_multi_schema_discovers_non_dbo_tables():
    result = _discover({})  # no schema pinned -> enumerate all user schemas
    by = {(t["schema"], t["name"]): t for t in result["tables"]}
    assert ("dbo", "orders") in by
    assert ("sales", "orders") in by
    assert ("sales", "inventory") in by
    assert result["total_tables_available"] == 4


def test_non_dbo_table_gets_its_pk_and_types():
    result = _discover({})
    by = {(t["schema"], t["name"]): t for t in result["tables"]}
    sales_orders = by[("sales", "orders")]
    assert sales_orders["primary_keys"] == ["id"]            # real PK, not synthetic
    ctypes = {c["name"]: c["type"] for c in sales_orders["columns"]}
    assert ctypes["id"] == "integer"                         # canonical, not all-String
    assert ctypes["amount"] == "decimal(12,2)"               # precision+scale preserved
    # the PK column is flagged on the column too
    id_col = next(c for c in sales_orders["columns"] if c["name"] == "id")
    assert id_col["is_primary_key"] is True


def test_same_named_tables_do_not_cross_assign():
    result = _discover({})
    by = {(t["schema"], t["name"]): t for t in result["tables"]}
    dbo_orders = by[("dbo", "orders")]
    dbo_cols = {c["name"] for c in dbo_orders["columns"]}
    # dbo.orders must keep ITS columns, not sales.orders' "amount"
    assert "label" in dbo_cols and "amount" not in dbo_cols
    assert dbo_orders["primary_keys"] == ["id"]


def test_explicit_schema_pins_discovery():
    result = _discover({"schema": "sales"})  # pinned -> dbo NOT scanned
    schemas = {t["schema"] for t in result["tables"]}
    assert schemas == {"sales"}
    assert result["total_tables_available"] == 2


if __name__ == "__main__":
    test_multi_schema_discovers_non_dbo_tables()
    test_non_dbo_table_gets_its_pk_and_types()
    test_same_named_tables_do_not_cross_assign()
    test_explicit_schema_pins_discovery()
    print("OK: sqlserver multi-schema discovery tests passed")
