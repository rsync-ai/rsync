"""Contract proof that the MySQL connector is a declared participant in the
DestinationLoadMixin load-strategy framework — NOT an undeclared parallel bulk
mechanism (the gap flagged when the multi-row bulk path was added).

The decision (vs PostgreSQL): MySQL's ``INSERT ... ON DUPLICATE KEY UPDATE`` is a
single-statement atomic upsert. PostgreSQL stages into a temp + merges only
because COPY cannot upsert (and ON CONFLICT errors on intra-statement dup keys).
MySQL needs neither staging nor dedupe, so it declares ``supports_staging=False``
and KEEPS its direct INSERT...ODKU executemany path — it must NOT be routed
through ``staged_upsert`` (that would double-write every row + a temp per flush
= a regression on the CDC/upsert hot path).

This asserts:
  1. the connector inherits DestinationLoadMixin (declared participant),
  2. load_spec declares multi_values / on_duplicate_key / supports_staging=False,
  3. supports_staged_load() is False  -> staged_upsert is bypassed (no regression),
  4. load_strategy_capability() exposes the SAME unified surface PG does.

Runs standalone against the REAL base_connector (pure-stdlib import; pyarrow et al.
are lazy). No DB, no network. Override BASE_CONNECTOR_PATH / MYSQL_CONNECTOR_PATH
to point at an image checkout.
"""
import os
import sys
import importlib.util

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO = os.path.dirname(_HERE)
BASE_PATH = os.environ.get(
    "BASE_CONNECTOR_PATH",
    os.path.join(_REPO, "shared", "mcp-connectors", "base_connector.py"),
)
CONN_PATH = os.environ.get(
    "MYSQL_CONNECTOR_PATH",
    os.path.join(
        _REPO, "shared", "mcp-connectors",
        "public", "database", "mysql", "versions", "v1.0.0", "connector.py",
    ),
)


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def main():
    # REAL base_connector so DestinationLoadMixin / DestinationLoadSpec behavior
    # (supports_staged_load, load_strategy_capability) is exercised for real.
    base = _load("base_connector", BASE_PATH)
    sys.modules["base_connector"] = base
    C = _load("mysql_conn", CONN_PATH)

    server = C.MysqlMCPServer()
    failures = []

    def check(label, cond, got=None):
        ok = bool(cond)
        print(f"[load-strategy] {'PASS' if ok else 'FAIL'}: {label}"
              + ("" if ok else f"  (got={got!r})"))
        if not ok:
            failures.append(label)

    # 1. declared participant in the framework
    check("connector inherits DestinationLoadMixin",
          isinstance(server, base.DestinationLoadMixin))

    # 2. spec declares MySQL's dialect strategy
    spec = getattr(server, "load_spec", None)
    check("has load_spec", spec is not None, spec)
    check("load_method == 'multi_values'",
          getattr(spec, "load_method", None) == "multi_values",
          getattr(spec, "load_method", None))
    check("merge_method == 'on_duplicate_key'",
          getattr(spec, "merge_method", None) == "on_duplicate_key",
          getattr(spec, "merge_method", None))

    # 3. THE regression guard: MySQL must NOT stage. supports_staging False ->
    #    supports_staged_load() False -> upsert_data keeps the direct ODKU path.
    check("load_spec.supports_staging is False",
          getattr(spec, "supports_staging", True) is False,
          getattr(spec, "supports_staging", None))
    check("supports_staged_load() is False (staged_upsert bypassed)",
          server.supports_staged_load() is False,
          server.supports_staged_load())

    # 4. unified capability surface, same shape PG exposes
    cap = server.load_strategy_capability()
    check("load_strategy_capability() is a dict with staged_load_available=False",
          isinstance(cap, dict) and cap.get("staged_load_available") is False, cap)
    check("capability advertises load_method 'multi_values'",
          cap.get("load_method") == "multi_values", cap.get("load_method"))

    if failures:
        print(f"[load-strategy] RESULT: FAIL ({len(failures)} checks) -> {failures}")
        sys.exit(1)
    print("[load-strategy] RESULT: PASS")
    sys.exit(0)


if __name__ == "__main__":
    main()
