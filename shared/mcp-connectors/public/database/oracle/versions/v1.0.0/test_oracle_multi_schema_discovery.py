"""Regression test: Oracle discovery enumerates tables across ALL owners/schemas
(not just the connecting login user's own objects) and keys columns/PKs by
(owner, table) so same-named tables in different schemas never cross-assign.

Before this fix, ``_discover_oracle_schema_v2`` queried the ``user_*``
data-dictionary views, which expose ONLY the login user's own objects, and
emitted an empty per-table ``schema`` field. A source table owned by another
schema (e.g. ``SALES.ORDERS`` while connected as ``RSYNCUSER``) was therefore
never discovered -> no column types or primary keys were threaded to the
destination -> every destination fell back to all-String columns + a synthetic
``_rsync_row_hash`` PK. This test pins the ``all_*`` cross-owner behavior.

Offline: a fake cursor answers the discovery queries by SQL shape + bind
params; no real Oracle / oracledb driver needed.

Run standalone (no pytest needed):
    python3 test_oracle_multi_schema_discovery.py

Or inside the built connector image:
    docker cp <thisfile> rsync-ai-oracle-v1-0-0-mcp:/app/ && \
    docker exec rsync-ai-oracle-v1-0-0-mcp python /app/test_oracle_multi_schema_discovery.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from connector import OracleMCPServer  # noqa: E402


def _srv():
    return OracleMCPServer.__new__(OracleMCPServer)  # bypass __init__ (no driver)


# owner, table_name, num_rows  (data dictionary order: owner, table_name)
_TABLES = [
    ("APP", "CUSTOMERS", 5),
    ("APP", "ORDERS", 10),
    ("SALES", "ORDERS", 20),
]
# owner, table_name, column_name, data_type, nullable, data_precision, data_scale
# (exercises canonical emission: NUMBER(p,0)->integer, NUMBER(p,s)->decimal,
#  VARCHAR2->string, DATE->timestamp)
_COLUMNS = [
    ("APP", "CUSTOMERS", "CID", "NUMBER", "N", 10, 0),
    ("APP", "ORDERS", "ID", "NUMBER", "N", 10, 0),
    ("APP", "ORDERS", "LABEL", "VARCHAR2", "Y", None, None),
    ("APP", "ORDERS", "CREATED", "DATE", "Y", None, None),
    ("SALES", "ORDERS", "ID", "NUMBER", "N", 10, 0),
    ("SALES", "ORDERS", "AMOUNT", "NUMBER", "N", 12, 2),
]
# owner, table_name, column_name  (primary keys)
_PKS = [
    ("APP", "CUSTOMERS", "CID"),
    ("APP", "ORDERS", "ID"),
    ("SALES", "ORDERS", "ID"),
]


class FakeCursor:
    """Answers the Oracle discovery query sequence from SQL shape + binds.

    ``fail_oracle_maintained`` simulates an older Oracle whose all_users view
    lacks the ``oracle_maintained`` column, forcing discovery down the plain
    owner-scan + Python denylist fallback path.
    """

    def __init__(self, fail_oracle_maintained=False):
        self._result = []
        self._one = None
        self.executed = []
        self.fail_oracle_maintained = fail_oracle_maintained

    @staticmethod
    def _str_binds(params):
        # Owner binds are strings; the table-list query appends an int
        # (max_tables) which we drop when matching owners.
        return [p for p in list(params or []) if isinstance(p, str)]

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        s = " ".join(sql.split())
        if "all_users" in s and "oracle_maintained" in s:
            # Primary owner-resolution path (oracle_maintained = 'N').
            if self.fail_oracle_maintained:
                raise Exception("ORA-00904: \"ORACLE_MAINTAINED\": invalid identifier")
            self._result = [("APP",), ("SALES",)]  # DB already excludes system owners
        elif "SELECT DISTINCT owner FROM all_tables" in s:
            # Fallback path: returns a system owner (SYS) the denylist must drop.
            self._result = [("APP",), ("SALES",), ("SYS",)]
        elif "SELECT COUNT(*) FROM all_tables WHERE owner IN" in s:
            owners = self._str_binds(params)
            self._one = [sum(1 for (o, _t, _n) in _TABLES if o in owners)]
        elif "FROM all_tables WHERE owner IN" in s and "num_rows" in s:
            owners = self._str_binds(params)
            self._result = [(o, t, n) for (o, t, n) in _TABLES if o in owners]
        elif "all_tab_columns" in s:
            self._result = list(_COLUMNS)  # connector filters by (owner, table)
        elif "all_constraints" in s and "'P'" in s:
            self._result = list(_PKS)
        elif "all_constraints" in s and "'R'" in s:
            self._result = []  # no foreign keys in this fixture
        elif "all_ind_columns" in s or "all_indexes" in s:
            self._result = []
        elif "SYS_CONTEXT" in s:
            self._one = ["APP"]
        else:
            self._result = []
            self._one = [0]

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._result

    def close(self):
        pass


def _discover(config, include_indexes=False, fail_oracle_maintained=False):
    srv = _srv()
    cur = FakeCursor(fail_oracle_maintained=fail_oracle_maintained)
    srv._get_cursor = lambda conn, as_dict=False: cur
    result = {}
    srv._discover_oracle_schema_v2(
        conn=object(), config=config, max_tables=100,
        include_columns=True, include_row_counts=True,
        include_relationships=True, include_indexes=include_indexes,
        result=result, add_warning=lambda *a, **k: None,
    )
    return result


def test_cross_owner_tables_discovered():
    result = _discover({})  # no owner pinned -> enumerate every non-system owner
    by = {(t["schema"], t["name"]): t for t in result["tables"]}
    assert ("APP", "ORDERS") in by
    assert ("APP", "CUSTOMERS") in by
    assert ("SALES", "ORDERS") in by
    assert result["total_tables_available"] == 3


def test_system_owner_excluded():
    result = _discover({})
    owners = {t["schema"] for t in result["tables"]}
    assert owners == {"APP", "SALES"}  # oracle_maintained excludes system owners


def test_fallback_denylist_filters_system_owners_on_old_oracle():
    # Older Oracle without all_users.oracle_maintained -> plain owner scan +
    # Python denylist must still drop SYS.
    result = _discover({}, fail_oracle_maintained=True)
    owners = {t["schema"] for t in result["tables"]}
    assert owners == {"APP", "SALES"}  # SYS dropped by the _is_system_owner denylist
    assert result["total_tables_available"] == 3


def test_foreign_owner_table_threads_pk_and_types():
    result = _discover({})
    by = {(t["schema"], t["name"]): t for t in result["tables"]}
    sales_orders = by[("SALES", "ORDERS")]
    assert sales_orders["primary_keys"] == ["ID"]
    ctypes = {c["name"]: c["type"] for c in sales_orders["columns"]}
    # Canonical types: NUMBER(10,0) -> integer, NUMBER(12,2) -> decimal.
    assert ctypes == {"ID": "integer", "AMOUNT": "decimal"}
    # PK column is flagged on the column object too.
    id_col = next(c for c in sales_orders["columns"] if c["name"] == "ID")
    assert id_col["is_primary_key"] is True


def test_types_are_canonical_not_raw_oracle():
    # The whole point of Phase 1: discovery emits CANONICAL types (so the
    # destination provisions real columns) instead of raw Oracle tokens.
    result = _discover({})
    by = {(t["schema"], t["name"]): t for t in result["tables"]}
    app_orders = {c["name"]: c for c in by[("APP", "ORDERS")]["columns"]}
    assert app_orders["ID"]["type"] == "integer"        # NUMBER(10,0)
    assert app_orders["LABEL"]["type"] == "string"      # VARCHAR2 (was raw before)
    assert app_orders["CREATED"]["type"] == "timestamp"  # Oracle DATE has a time part
    # raw token preserved for reference
    assert app_orders["LABEL"]["source_type"] == "VARCHAR2"
    assert app_orders["CREATED"]["source_type"] == "DATE"
    # every emitted type is in the canonical vocabulary
    from canonical_types import CANONICAL_TYPES
    for t in result["tables"]:
        for c in t["columns"]:
            assert c["type"] in CANONICAL_TYPES, (c["name"], c["type"])


def test_same_named_tables_do_not_cross_assign():
    result = _discover({})
    by = {(t["schema"], t["name"]): t for t in result["tables"]}
    app_cols = {c["name"] for c in by[("APP", "ORDERS")]["columns"]}
    sales_cols = {c["name"] for c in by[("SALES", "ORDERS")]["columns"]}
    # APP.ORDERS keeps ITS columns (LABEL), not SALES.ORDERS' AMOUNT.
    assert "LABEL" in app_cols and "AMOUNT" not in app_cols
    assert "AMOUNT" in sales_cols and "LABEL" not in sales_cols
    assert by[("APP", "ORDERS")]["primary_keys"] == ["ID"]


def test_schema_field_is_real_owner_not_empty():
    result = _discover({})
    for t in result["tables"]:
        assert t["schema"] in ("APP", "SALES")
        assert t["schema"] != ""  # the old bug emitted "" for every table


def test_explicit_owner_pins_and_upper_cases():
    # Lower-case config value must still match the upper-cased data dictionary.
    result = _discover({"schema": "sales"})
    schemas = {t["schema"] for t in result["tables"]}
    assert schemas == {"SALES"}
    names = {t["name"] for t in result["tables"]}
    assert names == {"ORDERS"}


if __name__ == "__main__":
    tests = [
        test_cross_owner_tables_discovered,
        test_system_owner_excluded,
        test_fallback_denylist_filters_system_owners_on_old_oracle,
        test_foreign_owner_table_threads_pk_and_types,
        test_types_are_canonical_not_raw_oracle,
        test_same_named_tables_do_not_cross_assign,
        test_schema_field_is_real_owner_not_empty,
        test_explicit_owner_pins_and_upper_cases,
    ]
    for t in tests:
        t()
        print(f"  ok  {t.__name__}")
    print("OK: oracle multi-schema discovery tests passed (%d)" % len(tests))
