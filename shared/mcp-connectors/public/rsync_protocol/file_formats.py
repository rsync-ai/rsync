from __future__ import annotations

import json
import mimetypes
from typing import Any, Dict, Iterable, Iterator, Optional


def iter_jsonl_bytes(records: Iterable[Dict[str, Any]]) -> Iterator[bytes]:
    """
    Stream records as NDJSON bytes, one object per line.
    """
    for r in records:
        if not isinstance(r, dict):
            continue
        yield (json.dumps(r, default=str) + "\n").encode("utf-8")


def guess_content_type(file_format: str) -> str:
    ff = (file_format or "").lower()
    if ff in ("jsonl", "ndjson"):
        return "application/x-ndjson"
    if ff == "json":
        return "application/json"
    if ff == "parquet":
        return "application/octet-stream"
    return "application/octet-stream"


# Canonical extension -> MIME map for the blob lane. Consulted BEFORE the stdlib
# `mimetypes` module so the common types resolve deterministically regardless of
# the host OS's mime database (mimetypes reads /etc/mime.types on Unix, the
# registry on Windows — the same extension can otherwise map differently across
# environments, breaking the plan's "deterministic content-type" invariant).
_CANONICAL_CONTENT_TYPES = {
    "pdf": "application/pdf",
    "csv": "text/csv",
    "tsv": "text/tab-separated-values",
    "json": "application/json",
    "jsonl": "application/x-ndjson",
    "ndjson": "application/x-ndjson",
    "txt": "text/plain",
    "log": "text/plain",
    "md": "text/markdown",
    "xml": "application/xml",
    "html": "text/html",
    "parquet": "application/vnd.apache.parquet",
    "avro": "application/avro",
    "orc": "application/orc",
    "arrow": "application/vnd.apache.arrow.file",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "png": "image/png",
    "jpg": "image/jpeg",
    "jpeg": "image/jpeg",
    "gif": "image/gif",
    "webp": "image/webp",
    "svg": "image/svg+xml",
    "tiff": "image/tiff",
    "mp4": "video/mp4",
    "mov": "video/quicktime",
    "mp3": "audio/mpeg",
    "zip": "application/zip",
    "gz": "application/gzip",
    "gzip": "application/gzip",
    "bz2": "application/x-bzip2",
}


def content_type_for_object(key: str, declared: Optional[str] = None) -> str:
    """Best-effort MIME type for a raw object copied through the blob (passthrough)
    lane — universal-blob-passthrough plan §2.

    Resolution order (the plan's fallback chain), chosen to be deterministic:
      1. a content-type the source store already declared (when available), else
      2. the canonical map above by file extension (stable across OSes), else
      3. the MIME mapped from the key's extension by stdlib ``mimetypes``, else
      4. ``application/octet-stream`` — the universal opaque-bytes type.

    Never raises: an unknown extension is a blob, not an error.
    """
    if isinstance(declared, str) and declared.strip():
        return declared.strip()
    name = (key or "")
    ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
    if ext in _CANONICAL_CONTENT_TYPES:
        return _CANONICAL_CONTENT_TYPES[ext]
    guessed, _ = mimetypes.guess_type(name)
    return guessed or "application/octet-stream"


def try_write_parquet_to_file(path: str, rows: Any) -> Optional[int]:
    """
    Best-effort Parquet writer. Returns bytes written or None if unavailable.
    Uses pyarrow if installed (many connectors already include it).
    """
    try:
        import pyarrow as pa  # type: ignore
        import pyarrow.parquet as pq  # type: ignore
    except Exception:
        return None

    if not isinstance(rows, list) or not rows:
        # Write empty parquet? keep simple: return 0
        return 0

    # Convert list-of-dicts to Arrow table
    table = pa.Table.from_pylist(rows)  # type: ignore[arg-type]
    pq.write_table(table, path)  # type: ignore[arg-type]

    try:
        import os

        return int(os.path.getsize(path))
    except Exception:
        return None


# =============================================================================
# READ SIDE — inverse of convert_data_to_format() / try_write_parquet_to_file().
#
# Object-storage SOURCE support: given the raw bytes of a stored object plus its
# format + (file-level) compression, return a list of row dicts. Used by the
# hand-built storage connectors (aws-s3, and later gcs/azure-blob) in their
# discover_schema() (sample → infer schema) and export() (parse → rows) paths.
#
# Type rule (v1, deliberate — keeps discover_schema DDL and export() values in
# lockstep so source→dest parity is exact):
#   - csv / tsv  → every column is "string"; values returned verbatim as strings
#                  (CSV is stringly-typed on the wire; typed-CSV inference is a
#                  Phase 5 enhancement and is intentionally NOT done here).
#   - json/jsonl → native JSON types preserved; column type inferred from values.
#   - parquet    → native types from the file; column type inferred from values.
# =============================================================================

# Formats whose values are inherently strings on read (no native typing).
_STRING_VALUE_FORMATS = ("csv", "tsv")


def decompress_bytes(content: bytes, compression: Optional[str]) -> bytes:
    """
    Reverse the file-level compression applied by convert_data_to_format().
    none/gzip/bzip2 are fully supported (stdlib); snappy/lz4/zstd are best-effort
    and raise a clear error if the framing/lib does not match. Parquet carries
    its own internal compression, so callers pass compression="none" for parquet.
    """
    if not content:
        return content
    c = (compression or "none").strip().lower()
    if c in ("", "none", "raw", "identity"):
        return content
    if c == "gzip":
        import gzip
        return gzip.decompress(content)
    if c in ("bzip2", "bz2"):
        import bz2
        return bz2.decompress(content)
    if c == "snappy":
        import snappy  # python-snappy
        return snappy.decompress(content)
    if c == "lz4":
        import lz4.frame  # type: ignore
        return lz4.frame.decompress(content)
    if c in ("zstd", "zstandard"):
        import zstandard  # type: ignore
        return zstandard.ZstdDecompressor().decompress(content)
    # Unknown codec: fail loud rather than silently returning compressed bytes.
    raise ValueError(f"Unsupported compression codec for read: {compression!r}")


def sniff_format_from_key(key: str) -> "tuple[str, str]":
    """
    Infer (file_format, compression) from an object key/extension. Used when the
    connector is configured with file_format="infer". A trailing .gz/.bz2 wins
    the compression slot and the preceding extension picks the format.
    Returns ("", "none") when nothing recognizable is found (caller decides).
    """
    name = (key or "").lower().strip()
    compression = "none"
    for suffix, codec in ((".gz", "gzip"), (".gzip", "gzip"), (".bz2", "bzip2"), (".bzip2", "bzip2")):
        if name.endswith(suffix):
            compression = codec
            name = name[: -len(suffix)]
            break
    fmt = ""
    if name.endswith((".jsonl", ".ndjson")):
        fmt = "jsonl"
    elif name.endswith(".json"):
        fmt = "json"
    elif name.endswith((".csv",)):
        fmt = "csv"
    elif name.endswith((".tsv", ".tab")):
        fmt = "tsv"
    elif name.endswith((".parquet", ".pq")):
        fmt = "parquet"
        compression = "none"  # parquet compression is internal, never file-level
    return fmt, compression


def _parse_json_bytes(raw: bytes) -> "list":
    """
    Parse a JSON document into a list of row dicts. Accepts:
      - a top-level array            → used directly
      - a single object             → wrapped as [obj]
      - {"data"|"records"|"results"|"items"|"rows": [...]} → that array
    """
    text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
    text = text.strip()
    if not text:
        return []
    doc = json.loads(text)
    if isinstance(doc, list):
        return [r for r in doc if isinstance(r, dict)]
    if isinstance(doc, dict):
        for k in ("data", "records", "results", "items", "rows"):
            v = doc.get(k)
            if isinstance(v, list):
                return [r for r in v if isinstance(r, dict)]
        return [doc]
    return []


def _parse_jsonl_bytes(raw: bytes) -> "list":
    text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
    out = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if isinstance(obj, dict):
            out.append(obj)
    return out


def _parse_delimited_bytes(raw: bytes, delimiter: str, quotechar: str = '"',
                           encoding: str = "utf-8") -> "list":
    import csv
    import io
    text = raw.decode(encoding, errors="replace") if isinstance(raw, (bytes, bytearray)) else str(raw)
    if not text.strip():
        return []
    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter, quotechar=quotechar or '"')
    out = []
    for row in reader:
        # csv.DictReader yields OrderedDict-like; normalize to plain dict, drop the
        # None key (extra trailing columns) so downstream JSON stays well-formed.
        out.append({k: v for k, v in row.items() if k is not None})
    return out


def _parse_parquet_bytes(raw: bytes) -> "list":
    import io
    import pyarrow.parquet as pq  # type: ignore
    table = pq.read_table(io.BytesIO(raw))
    return table.to_pylist()


def parse_bytes_to_rows(content: bytes, file_format: str, compression: str = "none") -> "list":
    """
    Inverse of convert_data_to_format(): raw object bytes → list of row dicts.
    Raises on a genuinely unparseable object so the caller can fail loud rather
    than silently dropping a file. See the type rule in the section header.
    """
    fmt = (file_format or "").strip().lower()
    if fmt in ("ndjson",):
        fmt = "jsonl"
    if fmt == "parquet":
        # parquet compression is internal; never double-decompress.
        return _parse_parquet_bytes(content)
    raw = decompress_bytes(content, compression)
    if fmt == "json":
        return _parse_json_bytes(raw)
    if fmt == "jsonl":
        return _parse_jsonl_bytes(raw)
    if fmt == "csv":
        return _parse_delimited_bytes(raw, delimiter=",")
    if fmt == "tsv":
        return _parse_delimited_bytes(raw, delimiter="\t")
    raise ValueError(f"Unsupported source file_format for parse: {file_format!r}")


def _infer_type(value: Any) -> str:
    """Map a single sampled value to a coarse column type used for dest DDL."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    return "string"


# Type-promotion lattice: null < boolean/integer < number < string. When sampled
# values disagree across rows, widen to the most permissive observed type so the
# dest column never rejects a value.
_TYPE_RANK = {"null": 0, "boolean": 1, "integer": 2, "number": 3, "string": 4}


def infer_schema_from_rows(rows: "list", file_format: str = "") -> "list":
    """
    Infer [{"name","type","nullable"}] columns from sampled row dicts. For
    csv/tsv every column is forced to "string" (see header type rule); for
    json/jsonl/parquet the type is the widened type observed across samples.
    Column order follows first-seen order across the sampled rows.
    """
    force_string = (file_format or "").strip().lower() in _STRING_VALUE_FORMATS
    order: "list" = []
    best: "dict" = {}
    nullable: "dict" = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        for k, v in row.items():
            if k is None:
                continue
            if k not in best:
                order.append(k)
                best[k] = "null"
                nullable[k] = False
            t = _infer_type(v)
            if t == "null":
                nullable[k] = True
                continue
            if _TYPE_RANK[t] > _TYPE_RANK.get(best[k], 0):
                best[k] = t
    columns = []
    for k in order:
        t = best.get(k, "null")
        if t == "null":
            t = "string"  # all-null column → safest dest type is text
        if force_string:
            t = "string"
        columns.append({"name": k, "type": t, "nullable": bool(nullable.get(k, True))})
    return columns

