"""Layout v2 files are parquet whatever format the connection was saved with.

The kafka-mcp-sink writes every layout v2 object as parquet: a hive/BigQuery table
folder needs one schema, one format. On GKE (v0.1.7 RC) every v2 file was gzip JSON
named ``.parquet``. The sink passed ``format="parquet"`` as a call argument, but the
object-storage connectors' ``_enforce_config_precedence`` lets the CONNECTION config
overwrite that argument, and ``prepare_destination_params`` maps a format it does not
know -- ``"infer"``, the gcs/azure-blob form default -- to json.

The sink now sends a config copy whose format keys say parquet
(``objectLayoutV2DestConfig``, kafka-sink-worker/object_layout_v2_write.go). Its Go
tests model the connector's precedence rule in ``connectorWrittenFormat``; this file
runs the connectors' REAL code, so a change to that rule fails here rather than
silently making the Go model a fiction.

Bug class: a call argument the connection config silently overrides.
"""
from __future__ import annotations

import importlib.util
import json
import os

import pytest

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_STORAGE = ("gcs", "aws-s3", "azure-blob")  # object_layout_golden.json v2_destinations

# Forms a user can save; the first is the gcs/azure-blob default the GKE run hit.
_CONNECTION_FORMATS = [
    {"file_format": "infer", "compression": "gzip"},
    {"file_format": "jsonl"},
    {"output_format": "csv", "compression": "bzip2"},
    {"format": "json", "compression": "zstd"},
]


def _connector(name):
    base = os.path.join(_ROOT, "public", "storage", name)
    with open(os.path.join(base, "latest.json")) as f:
        version = json.load(f)["current_version"]
    path = os.path.join(base, "versions", version, "base_connector.py")
    spec = importlib.util.spec_from_file_location(f"base_connector_v2fmt_{name.replace('-', '_')}", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    cls = mod.BaseMCPConnector
    stubs = {m: (lambda self, *a, **k: None) for m in cls.__abstractmethods__}
    stubs["log"] = lambda self, *a, **k: None
    return object.__new__(type("_V2FormatProbe", (cls,), stubs))


def _sink_args(config):
    """The call shape the sink sends for a v2 data write (codec gzip)."""
    return {
        "config": config,
        "key": "exports/sales/db/public/t/dt=2026-09-28/LOAD00000001.parquet",
        "bucket": "b",
        "data": [{"id": 1}],
        "format": "parquet",
        "file_format": "parquet",
        "compression": "gzip",
    }


def test_the_storage_list_matches_the_layout_golden():
    with open(os.path.join(_ROOT, "..", "object_layout_golden.json")) as f:
        golden = json.load(f)["v2_destinations"]
    canonical = {c.strip().lower().replace("_", "-") for c in golden["eligible"]}
    assert canonical == set(_STORAGE), f"v2 destinations are now {sorted(canonical)}; update _STORAGE"


@pytest.mark.parametrize("name", _STORAGE)
def test_premise_the_connection_config_beats_the_call_argument(name):
    """Why the sink must send the config copy. If this starts failing because the
    connector now honors the argument, objectLayoutV2DestConfig can be retired."""
    out = _connector(name).prepare_destination_params(_sink_args({"file_format": "infer", "compression": "gzip"}))
    assert (out["format"], out["compression"]) == ("json", "gzip"), out


@pytest.mark.parametrize("name", _STORAGE)
@pytest.mark.parametrize("saved", _CONNECTION_FORMATS, ids=lambda c: "+".join(f"{k}={v}" for k, v in c.items()))
def test_the_sinks_v2_config_copy_writes_parquet(name, saved):
    # Mirrors objectLayoutV2DestConfig: format keys forced, compression = the v2 codec.
    config = dict(saved, format="parquet", file_format="parquet", output_format="parquet", compression="gzip")
    out = _connector(name).prepare_destination_params(_sink_args(config))
    assert (out["format"], out["compression"]) == ("parquet", "gzip"), out
