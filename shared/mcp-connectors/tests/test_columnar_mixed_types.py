"""A mixed-type column must not fail a columnar write (B-MONGO-3).

A Mongo collection whose ``_id`` is an int in some documents and an ObjectId (a
string once serialised) in others reached the GCS destination as one batch, and
``pa.Table.from_pylist`` raised ArrowInvalid on it: 10,000 rows went to the DLQ.
``rsync_protocol.file_formats.normalize_rows_for_columnar`` now gives every column
one type before a parquet/orc/arrow write, and the three object-storage
destinations call it.

The normaliser decides from Python value kinds, so everything except the pyarrow
read-back runs without pyarrow (the CI llm job does not install it). The read-back
test carries its own control: the raw batch must still FAIL pyarrow, otherwise it
proves nothing.

This file lives in shared/mcp-connectors/tests/ because the CI llm job collects
this directory and does not collect rsync_protocol/test_*.py.
"""

from __future__ import annotations

import datetime as dt
import decimal
import importlib.util
import re
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[3]
_PUBLIC = _REPO / "shared" / "mcp-connectors" / "public"
_MODULE = _PUBLIC / "rsync_protocol" / "file_formats.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("rsync_file_formats_under_test", _MODULE)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


ff = _load_module()
norm = ff.normalize_rows_for_columnar

MIXED_ID = [
    {"_id": 1, "body": "a"},
    {"_id": "65f0c3a1b2c3d4e5f6a7b8c9", "body": "b"},
    {"_id": 2, "body": None},
]


def test_mixed_int_and_string_column_becomes_text():
    out = norm(MIXED_ID, "parquet")
    assert [r["_id"] for r in out] == ["1", "65f0c3a1b2c3d4e5f6a7b8c9", "2"]
    assert [r["body"] for r in out] == ["a", "b", None]  # untouched column, nulls kept
    assert len(out) == len(MIXED_ID)


def test_input_rows_are_not_mutated():
    rows = [dict(r) for r in MIXED_ID]
    norm(rows, "parquet")
    assert rows == MIXED_ID


@pytest.mark.parametrize("fmt", ["json", "jsonl", "csv", "tsv", "avro", "xlsx", "", None])
def test_row_formats_are_returned_unchanged(fmt):
    assert norm(MIXED_ID, fmt) is MIXED_ID


@pytest.mark.parametrize("fmt", ["parquet", "PARQUET", " orc ", "arrow"])
def test_every_columnar_format_is_normalised(fmt):
    assert norm(MIXED_ID, fmt)[0]["_id"] == "1"


def test_agreeing_batch_is_the_same_object():
    rows = [{"a": 1, "b": "x", "c": {"k": 1}}, {"a": 2, "b": None, "c": {"k": 2}}]
    assert norm(rows, "parquet") is rows


def test_int_and_float_stay_numeric():
    rows = [{"n": 1}, {"n": 2.5}]
    assert norm(rows, "parquet") is rows


def test_bool_and_int_do_not_count_as_one_kind():
    out = norm([{"f": True}, {"f": 3}], "parquet")
    assert [r["f"] for r in out] == ["true", "3"]


def test_scalar_and_object_mix_becomes_json_text():
    out = norm([{"m": "plain"}, {"m": {"k": [1, 2]}}, {"m": [1]}], "parquet")
    assert [r["m"] for r in out] == ["plain", '{"k": [1, 2]}', "[1]"]


def test_declared_string_column_is_text_even_when_the_batch_agrees():
    # Files of one table must agree with the declared DDL: an all-int batch of a
    # column declared string would otherwise write an int64 column next to string
    # files, and a hive/BigQuery external table rejects the mismatch.
    rows = [{"_id": 1, "n": 1}, {"_id": 2, "n": 2}]
    out = norm(rows, "parquet", {"_id": "string", "n": "integer"})
    assert [r["_id"] for r in out] == ["1", "2"]
    assert [r["n"] for r in out] == [1, 2]


@pytest.mark.parametrize("declared", ["VARCHAR(255)", "character varying", "text", "uuid", "ObjectId"])
def test_declared_string_dialects(declared):
    assert norm([{"c": 7}], "parquet", {"c": declared})[0]["c"] == "7"


@pytest.mark.parametrize("declared", ["integer", "jsonb", "timestamp", None, 5])
def test_non_string_declarations_leave_an_agreeing_column(declared):
    rows = [{"c": 7}]
    assert norm(rows, "parquet", {"c": declared}) is rows


def test_text_rendering_of_other_kinds():
    rows = [
        {"c": "s"},
        {"c": dt.datetime(2026, 1, 2, 3, 4, 5)},
        {"c": dt.date(2026, 1, 2)},
        {"c": decimal.Decimal("1.50")},
        {"c": b"\x01\xff"},
    ]
    assert [r["c"] for r in norm(rows, "parquet")] == [
        "s", "2026-01-02T03:04:05", "2026-01-02", "1.50", "01ff"]


def test_non_dict_rows_and_bad_column_types_are_tolerated():
    rows = [{"a": 1}, "not-a-row", {"a": "x"}]
    out = norm(rows, "parquet", column_types="not-a-map")
    assert out[1] == "not-a-row" and out[0]["a"] == "1" and out[2]["a"] == "x"


def test_warning_never_carries_a_value(caplog):
    # The column name may itself show as [redacted]: the connector log scrubber
    # (base_connector.py) redacts quoted tokens once it is installed in the process.
    secret = "user-secret-value-123"
    with caplog.at_level("WARNING"):
        norm([{"ref_code": 5}, {"ref_code": secret}], "parquet")
    assert "written as text" in caplog.text
    assert secret not in caplog.text


# ---------------------------------------------------------------- real pyarrow

def test_pyarrow_reads_back_what_it_rejected_before():
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    import io

    # Control: the raw batch is the one that sent 10,000 rows to the DLQ.
    with pytest.raises((pa.ArrowInvalid, pa.ArrowTypeError)):
        pa.Table.from_pylist(MIXED_ID)

    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(norm(MIXED_ID, "parquet")), buf)
    back = pq.read_table(io.BytesIO(buf.getvalue()))
    assert back.schema.field("_id").type == pa.string()
    assert back.column("_id").to_pylist() == ["1", "65f0c3a1b2c3d4e5f6a7b8c9", "2"]


def test_nested_shapes_pyarrow_cannot_merge_become_json_text():
    pa = pytest.importorskip("pyarrow")
    rows = [{"m": {"k": 1}}, {"m": {"k": "x"}}]
    with pytest.raises((pa.ArrowInvalid, pa.ArrowTypeError)):
        pa.Table.from_pylist(rows)
    out = norm(rows, "parquet")
    assert [r["m"] for r in out] == ['{"k": 1}', '{"k": "x"}']
    pa.Table.from_pylist(out)


def test_try_write_parquet_to_file_normalises(tmp_path):
    pytest.importorskip("pyarrow")
    n = ff.try_write_parquet_to_file(str(tmp_path / "p.parquet"), [dict(r) for r in MIXED_ID])
    assert n and n > 0


# ---------------------------------------------------------------- wiring guard

@pytest.mark.parametrize("connector", ["gcs", "aws-s3", "azure-blob"])
def test_object_storage_destinations_normalise_before_every_columnar_write(connector):
    import json

    latest = json.loads((_PUBLIC / "storage" / connector / "latest.json").read_text())
    src = (_PUBLIC / "storage" / connector / "versions" / latest["current_version"]
           / "connector.py").read_text()
    writes = [m.start() for m in re.finditer(r"self\.convert_data_to_format\(", src)]
    assert writes, f"{connector}: no convert_data_to_format call found — guard is stale"
    for pos in writes:
        before = src[max(0, pos - 400):pos]
        assert "normalize_rows_for_columnar(" in before, (
            f"{connector}: convert_data_to_format at offset {pos} is not preceded by "
            "normalize_rows_for_columnar — a mixed-type column fails the batch")
    for m in re.finditer(r"try_write_parquet_to_file\(([^)]*)\)", src):
        assert "column_types" in m.group(1), f"{connector}: {m.group(0)} drops column_types"
