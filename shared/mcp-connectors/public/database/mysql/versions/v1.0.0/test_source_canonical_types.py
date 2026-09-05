"""Regression lock: the MySQL source's `_canonical_mysql_type` delegates to the
shared `canonicalize_type` authority (canonical_types.py).

`discover_schema` (`_discover_mysql_schema_v2`) emits each column's canonical
type via `_canonical_mysql_type`, which now routes through the single shared
authority instead of a per-connector table. Behavior-preserving for the common
tokens; strictly fixes the ones the old local table missed, verified by a
56-identical / 22 "string"->correct equivalence sweep. The one token that would
have regressed (`varbinary`: binary -> string) was fixed by ADDING
`varbinary -> binary` to the shared flat map, so it stays binary here.

Run inside the mysql connector image:
    docker cp <thisfile> rsync-ai-mysql-v1-0-0-mcp:/app/ && \
    docker exec rsync-ai-mysql-v1-0-0-mcp python -m pytest /app/test_source_canonical_types.py -q
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from connector import _canonical_mysql_type  # noqa: E402


def test_common_mysql_tokens_unchanged():
    assert _canonical_mysql_type("varchar(255)") == "string"
    assert _canonical_mysql_type("longtext") == "string"
    assert _canonical_mysql_type("bigint") == "integer"
    assert _canonical_mysql_type("tinyint") == "integer"
    assert _canonical_mysql_type("double") == "number"
    assert _canonical_mysql_type("decimal(12,4)") == "decimal"
    assert _canonical_mysql_type("tinyint") == "integer"
    assert _canonical_mysql_type("datetime") == "timestamp"
    assert _canonical_mysql_type("date") == "date"
    assert _canonical_mysql_type("time") == "time"
    assert _canonical_mysql_type("json") == "json"
    # binary family — incl. varbinary, which the shared flat map now covers.
    assert _canonical_mysql_type("blob") == "binary"
    assert _canonical_mysql_type("binary") == "binary"
    assert _canonical_mysql_type("varbinary") == "binary"
    assert _canonical_mysql_type("bit") == "binary"


def test_previously_missed_tokens_now_fold_correctly():
    # Fell to "string" in the old local table; the shared authority resolves them.
    for t in ("int2", "int4", "int8", "serial", "bigserial", "smallserial"):
        assert _canonical_mysql_type(t) == "integer", t
    assert _canonical_mysql_type("money") == "decimal"
    assert _canonical_mysql_type("double precision") == "number"
    assert _canonical_mysql_type("bit varying") == "binary"
    assert _canonical_mysql_type("jsonb") == "json"


def test_unknown_falls_back_to_string():
    assert _canonical_mysql_type("totally_unknown_type") == "string"
    assert _canonical_mysql_type("") == "string"
    assert _canonical_mysql_type(None) == "string"


if __name__ == "__main__":
    test_common_mysql_tokens_unchanged()
    test_previously_missed_tokens_now_fold_correctly()
    test_unknown_falls_back_to_string()
    print("OK: mysql source _canonical_mysql_type delegation tests passed")
