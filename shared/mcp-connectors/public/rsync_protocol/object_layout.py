"""Object-storage key layout v2 (L6): pure key builders, not yet called by any connector.

The layout is
``<conn prefix>/<pipeline prefix>/<db>/[<schema>/]<table>/dt=YYYY-MM-DD/LOAD00000001.parquet``
for batch loads and ``.../dt=YYYY-MM-DD/<YYYYMMDD-HHMMSSmmm>[-p<n>]-<first offset>.parquet``
for CDC files. ``_MANIFEST.json`` and ``_SUCCESS`` live under
``<conn prefix>/<pipeline prefix>/_rsync/<db>/[<schema>/]<table>/dt=YYYY-MM-DD/``.

This is one of three copies pinned by ``shared/object_layout_golden.json``; the others are
``backend-orchestrator/internal/storage/layout.go`` and the kafka-sink-worker's
``object_layout.go``. Only v2 is ported here: the v1 keys are written by the Go worker,
and no Python code builds them.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone

_SPACE = " \t\n\r\v\f"
_PIPELINE_PREFIX_RE = re.compile(r"[a-z0-9][a-z0-9_-]{0,62}")
_DT_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")

_DB_AND_SCHEMA = ("postgresql", "sqlserver", "oracle")
_DB_ONLY = ("mongodb", "mysql")


class ObjectLayoutError(ValueError):
    """A key cannot be built; ``code`` is the stable code shared with the golden file."""

    def __init__(self, code: str) -> None:
        super().__init__(f"object layout v2: {code}")
        self.code = code


@dataclass(frozen=True)
class LayoutV2Table:
    """One table's place in the v2 layout.

    ``source_family`` sets the namespace depth: postgresql/sqlserver/oracle use
    ``<db>/<schema>``, mongodb/mysql use ``<db>``, and ``""`` (a non-database source)
    uses neither.
    """

    conn_prefix: str = ""
    pipeline_prefix: str = ""
    source_family: str = ""
    database: str = ""
    schema: str = ""
    table: str = ""


def encode_name(name: str) -> str:
    """Percent-encode, byte by byte, ``%``, ``/``, ``\\``, ``=``, control bytes, and a
    leading ``_`` or ``.``. Everything else, case included, is kept."""
    out = bytearray()
    for i, c in enumerate(name.encode("utf-8")):
        if c in b"%/\\=" or c < 0x20 or c == 0x7F or (i == 0 and c in b"_."):
            out += b"%%%02X" % c
        else:
            out.append(c)
    return out.decode("utf-8")


def pipeline_root_prefix(conn_prefix: str, pipeline_prefix: str) -> str:
    if not _PIPELINE_PREFIX_RE.fullmatch(pipeline_prefix):
        raise ObjectLayoutError("pipeline_prefix_invalid")
    conn = conn_prefix.strip(_SPACE).strip("/")
    return f"{conn}/{pipeline_prefix}/" if conn else f"{pipeline_prefix}/"


def _namespace(t: LayoutV2Table) -> str:
    db = t.database.strip(_SPACE)
    schema = t.schema.strip(_SPACE)
    table = t.table.strip(_SPACE)
    family = t.source_family.strip(_SPACE).lower()
    parts: list[str] = []
    if family in _DB_AND_SCHEMA:
        if not db:
            raise ObjectLayoutError("database_required")
        if not schema:
            raise ObjectLayoutError("schema_required")
        parts += [db, schema]
    elif family in _DB_ONLY:
        if not db:
            raise ObjectLayoutError("database_required")
        parts.append(db)
    elif family != "":
        raise ObjectLayoutError("source_family_unknown")
    if not table:
        raise ObjectLayoutError("table_required")
    parts.append(table)
    return "".join(encode_name(p) + "/" for p in parts)


def table_prefix(t: LayoutV2Table) -> str:
    root = pipeline_root_prefix(t.conn_prefix, t.pipeline_prefix)
    return root + _namespace(t)


def sidecar_table_prefix(t: LayoutV2Table) -> str:
    root = pipeline_root_prefix(t.conn_prefix, t.pipeline_prefix)
    return root + "_rsync/" + _namespace(t)


def _check_dt(dt: str) -> None:
    if not _DT_RE.fullmatch(dt):
        raise ObjectLayoutError("dt_invalid")
    try:
        datetime.strptime(dt, "%Y-%m-%d")
    except ValueError:
        raise ObjectLayoutError("dt_invalid") from None


def load_key(t: LayoutV2Table, dt: str, load_seq: int) -> str:
    prefix = table_prefix(t)
    _check_dt(dt)
    if load_seq < 1 or load_seq > 99_999_999:
        raise ObjectLayoutError("load_seq_out_of_range")
    return f"{prefix}dt={dt}/LOAD{load_seq:08d}.parquet"


def cdc_key(t: LayoutV2Table, ts_ms: int, partition: int, first_offset: int) -> str:
    prefix = table_prefix(t)
    # Upper bound: 10000-01-01T00:00:00Z would render a 5-digit year.
    if ts_ms <= 0 or ts_ms >= 253_402_300_800_000:
        raise ObjectLayoutError("ts_invalid")
    if partition < 0:
        raise ObjectLayoutError("partition_invalid")
    if first_offset < 0:
        raise ObjectLayoutError("offset_invalid")
    secs, millis = divmod(ts_ms, 1000)
    ts = datetime.fromtimestamp(secs, tz=timezone.utc)
    name = ts.strftime("%Y%m%d-%H%M%S") + f"{millis:03d}"
    if partition > 0:
        name += f"-p{partition}"
    return f"{prefix}dt={ts.strftime('%Y-%m-%d')}/{name}-{first_offset}.parquet"


def _sidecar_key(t: LayoutV2Table, dt: str, leaf: str) -> str:
    prefix = sidecar_table_prefix(t)
    _check_dt(dt)
    return f"{prefix}dt={dt}/{leaf}"


def manifest_key(t: LayoutV2Table, dt: str) -> str:
    return _sidecar_key(t, dt, "_MANIFEST.json")


def success_key(t: LayoutV2Table, dt: str) -> str:
    return _sidecar_key(t, dt, "_SUCCESS")
