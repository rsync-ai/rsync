"""Regression test: PostgreSQL discovery enumerates ALL user schemas (not just
`public`) and keys columns/PKs by (schema, table) so same-named tables in
different schemas never cross-assign.

Before this fix, `_discover_postgres_schema_v2` scanned only
`config.get("schema", "public")`. A source connection that omitted `schema`
never discovered tables in other schemas (e.g. `shop.orders`), so no column
types or primary keys were threaded to the destination -> every destination
fell back to all-TEXT/String columns + a synthetic row-hash PK.

Offline: a fake cursor answers the discovery queries by SQL shape; no real
Postgres needed.

Run inside the postgresql connector image:
    docker cp <thisfile> rsync-ai-postgresql-v1-0-0-mcp:/app/ && \
    docker exec rsync-ai-postgresql-v1-0-0-mcp python /app/test_multi_schema_discovery.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from connector import PostgresqlMCPServer  # noqa: E402


def _srv():
    return PostgresqlMCPServer.__new__(PostgresqlMCPServer)  # bypass __init__


class FakeCursor:
    """Answers the discovery query sequence purely from SQL shape + bind params."""

    def __init__(self):
        self._result = []
        self._one = None
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        s = " ".join(sql.split())
        if "information_schema.schemata" in s:
            self._result = [("public",), ("shop",)]
        elif "SELECT COUNT(*) FROM information_schema.tables" in s:
            sch = (params or [None])[0]
            self._one = [2] if sch in ("public", "shop") else [0]
        elif "FROM pg_class c" in s and "pg_namespace" in s:
            sch = (params or [None])[0]
            if sch == "public":
                self._result = [("orders", 10), ("users", 5)]
            elif sch == "shop":
                self._result = [("orders", 20), ("inventory", 7)]
            else:
                self._result = []
        elif "information_schema.columns" in s:
            # (table_schema, table_name, column_name, data_type, is_nullable, prec, scale)
            self._result = [
                ("public", "orders", "id", "integer", "NO", None, None),
                ("public", "orders", "label", "text", "YES", None, None),
                ("public", "users", "uid", "bigint", "NO", None, None),
                ("shop", "orders", "id", "bigint", "NO", None, None),
                ("shop", "orders", "amount", "numeric", "NO", 12, 2),
                ("shop", "inventory", "sku", "text", "NO", None, None),
            ]
        elif "PRIMARY KEY" in s:
            # (table_schema, table_name, column_name)
            self._result = [
                ("public", "orders", "id"),
                ("public", "users", "uid"),
                ("shop", "orders", "id"),
            ]
        elif "FOREIGN KEY" in s:
            self._result = []
        elif "pg_indexes" in s:
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
    srv._discover_postgres_schema_v2(
        conn=object(), config=config, max_tables=100,
        include_columns=True, include_row_counts=True,
        include_relationships=True, include_indexes=False,
        result=result, add_warning=lambda *a, **k: None,
    )
    return result


def test_multi_schema_discovers_non_public_tables():
    result = _discover({})  # no schema pinned -> enumerate all user schemas
    by = {(t["schema"], t["name"]): t for t in result["tables"]}
    assert ("public", "orders") in by
    assert ("shop", "orders") in by
    assert ("shop", "inventory") in by
    assert result["total_tables_available"] == 4


def test_non_public_table_gets_its_pk_and_types():
    result = _discover({})
    by = {(t["schema"], t["name"]): t for t in result["tables"]}
    shop_orders = by[("shop", "orders")]
    assert shop_orders["primary_keys"] == ["id"]
    ctypes = {c["name"]: c["type"] for c in shop_orders["columns"]}
    assert ctypes["id"] == "integer"            # bigint -> canonical integer
    assert ctypes["amount"] == "numeric(12,2)"  # precision+scale preserved


def test_same_named_tables_do_not_cross_assign():
    result = _discover({})
    by = {(t["schema"], t["name"]): t for t in result["tables"]}
    pub_orders = by[("public", "orders")]
    pub_cols = {c["name"] for c in pub_orders["columns"]}
    # public.orders must keep ITS columns, not shop.orders' "amount"
    assert "label" in pub_cols and "amount" not in pub_cols
    assert pub_orders["primary_keys"] == ["id"]


def test_explicit_schema_pins_discovery():
    result = _discover({"schema": "shop"})  # pinned -> public NOT scanned
    schemas = {t["schema"] for t in result["tables"]}
    assert schemas == {"shop"}


if __name__ == "__main__":
    test_multi_schema_discovers_non_public_tables()
    test_non_public_table_gets_its_pk_and_types()
    test_same_named_tables_do_not_cross_assign()
    test_explicit_schema_pins_discovery()
    print("OK: postgresql multi-schema discovery tests passed")
