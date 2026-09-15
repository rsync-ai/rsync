"""Regression tests: a parquet file carries its codec INSIDE, never as a wrapper.

Two defects lived in `convert_data_to_format`'s parquet path, both silent:

1. The finished parquet bytes were handed to `_compress_data` like any other format, so
   a `parquet` + `gzip` config produced a gzip-wrapped parquet — a blob whose magic
   bytes and footer are no longer where the format says, which BigQuery, hive and
   pyarrow all refuse to read. Parquet's codec is per column chunk, in its own footer.
2. Every bronze object was written UNCOMPRESSED, on a CDC envelope of highly repetitive
   JSON, so each one was billed twice — as storage, and as bytes scanned by every query
   over it. The root cause was NOT the writer: the writer did what it was told. The
   three cloud-storage connectors shipped `"compression": {"default": "none"}`, the
   connection form persisted that, and the form then displayed `none` back. The fix is
   a real default in the schema, guarded by
   `test_cloud_storage_connectors_default_to_a_real_codec` — never a writer that
   silently substitutes a codec the stored config doesn't name.

The call-contract tests below run a fake pyarrow, so they are a real guard on any
machine, installed or not — the codec assertion never degrades into a skip. The
read-back test additionally proves the codec reaches the file's footer, and is the one
that skips when pyarrow is absent. `test_non_parquet_format_still_gets_the_wrapper` is
the positive control: without it, a `_compress_data` that had stopped running entirely
would pass every other assertion here.
"""
from __future__ import annotations

import gzip
import importlib.util
import io
import os
import sys

import pytest

_HERE = os.path.dirname(__file__)
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
_BC_PATH = os.path.join(_ROOT, "base_connector.py")
_spec = importlib.util.spec_from_file_location("base_connector_pqc", _BC_PATH)
_bc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_bc)

BaseMCPConnector = _bc.BaseMCPConnector

_FAKE_BODY = b"PAR1fake-parquet-bodyPAR1"
_ROWS = [{"after_json": '{"a":1}', "op": "I"}, {"after_json": '{"a":2}', "op": "U"}]


def _connector():
    """A concrete, un-__init__'d BaseMCPConnector — the format path needs no state."""
    stubs = {name: (lambda self, *a, **k: None) for name in BaseMCPConnector.__abstractmethods__}
    concrete = type("_ConcreteForFormatTests", (BaseMCPConnector,), stubs)
    return object.__new__(concrete)


@pytest.fixture
def write_table_kwargs(monkeypatch):
    """Install a fake pyarrow and capture the kwargs `write_table` is called with."""
    captured = []

    class _FakeParquet:
        @staticmethod
        def write_table(table, buffer, **kwargs):
            captured.append(kwargs)
            buffer.write(_FAKE_BODY)

    class _FakeTable:
        @staticmethod
        def from_pylist(rows):
            return rows

    fake_pa = type(sys)("pyarrow")
    fake_pa.table = lambda mapping: mapping
    fake_pa.Table = _FakeTable
    fake_pa.parquet = _FakeParquet
    monkeypatch.setitem(sys.modules, "pyarrow", fake_pa)
    monkeypatch.setitem(sys.modules, "pyarrow.parquet", _FakeParquet)
    return captured


def test_none_writes_no_codec_because_that_is_what_it_says(write_table_kwargs):
    # 'none' must mean none — the same as it does for every other format. The connection
    # form stores this value and displays it back, so substituting pyarrow's default
    # here would make the stored config disagree with the bytes on disk, which is the
    # harder bug of the two. The sensible default belongs in the connector's schema; see
    # test_cloud_storage_connectors_default_to_a_real_codec below.
    out = _connector().convert_data_to_format(_ROWS, "parquet", "none")
    assert out == _FAKE_BODY
    assert write_table_kwargs == [{"compression": "none"}], write_table_kwargs


def test_explicit_codec_goes_inside_the_file_not_around_it(write_table_kwargs):
    out = _connector().convert_data_to_format(_ROWS, "parquet", "gzip")
    assert write_table_kwargs == [{"compression": "gzip"}]
    # The bytes leave unwrapped: identical to what write_table produced, and not a gzip
    # stream. `out[:2] != b"\x1f\x8b"` alone would pass on an empty result, so compare
    # the whole body.
    assert out == _FAKE_BODY
    assert not out.startswith(b"\x1f\x8b")


def test_uncompressed_is_still_reachable_explicitly(write_table_kwargs):
    # Config spells the deliberate opt-out 'uncompressed' ('none' already means unset);
    # pyarrow spells it 'none'. The translation happens at the write call.
    _connector().convert_data_to_format(_ROWS, "parquet", "uncompressed")
    assert write_table_kwargs == [{"compression": "none"}]


def test_codec_parquet_cannot_store_is_rejected_not_silently_wrapped(write_table_kwargs):
    # bzip2 is a valid _compress_data codec but not a parquet one. Before the fix this
    # produced a bzip2-wrapped parquet nothing could read; strict mode is the contract.
    with pytest.raises(ValueError, match="Parquet"):
        _connector().convert_data_to_format(_ROWS, "parquet", "bzip2")
    assert write_table_kwargs == []


def test_non_parquet_format_still_gets_the_wrapper():
    # Positive control for the exclusion above: jsonl + gzip must still be wrapped.
    out = _connector().convert_data_to_format(_ROWS, "jsonl", "gzip")
    assert out.startswith(b"\x1f\x8b")
    assert b'"op": "I"' in gzip.decompress(out)


def test_codec_is_recorded_in_the_real_files_footer():
    pq = pytest.importorskip("pyarrow.parquet", reason="pyarrow not installed")
    out = _connector().convert_data_to_format(_ROWS, "parquet", "gzip")
    meta = pq.read_metadata(io.BytesIO(out))
    codecs = {meta.row_group(0).column(i).compression for i in range(meta.num_columns)}
    assert meta.num_columns > 0
    assert codecs == {"GZIP"}, codecs
    assert pq.read_table(io.BytesIO(out)).num_rows == len(_ROWS)


def test_every_advertised_codec_is_one_real_pyarrow_accepts():
    """PARQUET_COMPRESSION_CODECS is the contract; pyarrow is the only authority on it.

    The fake above would happily "accept" a codec pyarrow rejects — which is exactly how
    'uncompressed' (pyarrow spells it 'none') shipped as an advertised-but-fatal option
    until a real write caught it. Every name the class advertises gets written and read
    back here.
    """
    pq = pytest.importorskip("pyarrow.parquet", reason="pyarrow not installed")
    expected = {"none": "UNCOMPRESSED", "uncompressed": "UNCOMPRESSED"}
    for name in BaseMCPConnector.PARQUET_COMPRESSION_CODECS:
        out = _connector().convert_data_to_format(_ROWS, "parquet", name)
        meta = pq.read_metadata(io.BytesIO(out))
        codecs = {meta.row_group(0).column(i).compression for i in range(meta.num_columns)}
        assert codecs == {expected.get(name, name.upper())}, (name, codecs)
        assert pq.read_table(io.BytesIO(out)).num_rows == len(_ROWS), name


def test_every_vendored_copy_carries_the_same_fix():
    """The 23 copies drift; this keeps the parquet path from drifting back."""
    copies = [
        os.path.join(dirpath, "base_connector.py")
        for dirpath, _, files in os.walk(_ROOT)
        if "base_connector.py" in files
    ]
    # Positive denominator: an empty census would otherwise pass every assertion below.
    assert len(copies) >= 20, f"only found {len(copies)} copies — census is broken"
    for path in copies:
        src = open(path).read()
        rel = os.path.relpath(path, _ROOT)
        assert "pq.write_table(table, buffer, compression=None)" not in src, rel
        assert "content = self._convert_to_parquet(data, compression)" in src, rel
        assert (
            "if compression and compression != 'none' and format_val != 'parquet':" in src
        ), rel
        # And no copy may reintroduce the "'none' really means snappy" branch: the
        # writer must do literally what the stored config says.
        assert "if codec == 'none':" not in src, rel


# Codecs _compress_data implements, and the pip package each one needs. gzip and bzip2
# are stdlib; the rest are third-party, so a connector may only default to them if its
# own requirements.txt installs the library.
_WRAPPER_CODEC_REQUIREMENTS = {
    "gzip": None,
    "bzip2": None,
    "snappy": "python-snappy",
    "lz4": "lz4",
    "zstd": "zstandard",
}

_CLOUD_STORAGE = ("gcs", "aws-s3", "azure-blob")


def test_cloud_storage_connectors_default_to_a_real_codec():
    """The schema default is the ONLY honest place to state a compression preference.

    The connection form seeds each field from its schema `default` and persists what it
    seeds (GenericConnectorForm.tsx:315-323), so `"default": "none"` is exactly why a
    live GCS connection stored — and displayed — `Compression: none` while writing
    uncompressed bronze objects. Whatever this default says is what the user sees and
    what the writer does; the two can only agree if the default names a real codec.

    The default must also survive every format the same connector offers, because one
    `compression` field covers all of them: valid as an internal parquet codec, valid as
    an external wrapper, and backed by a library this connector actually installs.
    """
    import json

    storage_root = os.path.join(_ROOT, "public", "storage")
    checked = []
    for name in _CLOUD_STORAGE:
        conn_dir = os.path.join(storage_root, name)
        current = json.load(open(os.path.join(conn_dir, "latest.json")))["current_version"]
        vdir = os.path.join(conn_dir, "versions", current)
        meta = json.load(open(os.path.join(vdir, "metadata.json")))
        requirements = open(os.path.join(vdir, "requirements.txt")).read().lower()

        # Both blocks: the form reads configuration_schema, other callers read
        # config_schema, and a default that lands in only one of them is a drift bug.
        for block in ("configuration_schema", "config_schema"):
            props = meta[block]["properties"]
            default = props["compression"]["default"]
            where = f"{name}/{current}/{block}"

            assert default not in ("none", "uncompressed", ""), (
                f"{where}: compression defaults to {default!r}, so every object is "
                f"written raw and the form tells the user so — state a real codec"
            )
            assert default in props["compression"]["enum"], (
                f"{where}: default {default!r} is not in its own enum, so the form's "
                f"select cannot even display it"
            )
            assert default in BaseMCPConnector.PARQUET_COMPRESSION_CODECS, (
                f"{where}: default {default!r} is not a codec parquet can store, so "
                f"file_format=parquet would fail outright"
            )
            pkg = _WRAPPER_CODEC_REQUIREMENTS.get(default)
            assert default in _WRAPPER_CODEC_REQUIREMENTS, (
                f"{where}: default {default!r} is not a codec _compress_data implements, "
                f"so every non-parquet format would fail"
            )
            assert pkg is None or pkg in requirements, (
                f"{where}: default {default!r} needs {pkg}, which is not in this "
                f"connector's requirements.txt"
            )
            checked.append(where)

    # Positive denominator: an empty or short census would pass every assertion above.
    assert len(checked) == 2 * len(_CLOUD_STORAGE), checked
