"""Pins rsync_protocol/object_layout.py to shared/object_layout_golden.json (v2 block).

The same fixture pins backend-orchestrator/internal/storage/layout.go and the
kafka-sink-worker's object_layout.go, so a layout change that lands in one copy and not
the others fails here. The v1 block describes keys only the Go worker writes; this test
checks that the block exists but does not run it.

This file lives in shared/mcp-connectors/tests/ (not next to the module) because the CI
llm job collects this directory and does not collect rsync_protocol/test_*.py.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from dataclasses import fields
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[3]
_GOLDEN = _REPO / "shared" / "object_layout_golden.json"
_MODULE = _REPO / "shared" / "mcp-connectors" / "public" / "rsync_protocol" / "object_layout.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("rsync_object_layout_under_test", _MODULE)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    # @dataclass resolves annotations through sys.modules, so register before exec.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


layout = _load_module()
_TABLE_FIELDS = {f.name for f in fields(layout.LayoutV2Table)}
_EXTRA_FIELDS = {"dt", "load_seq", "ts_ms", "partition", "first_offset"}


def _table(inp: dict):
    return layout.LayoutV2Table(**{k: v for k, v in inp.items() if k in _TABLE_FIELDS})


_RUNNERS = {
    "pipeline_root_prefix": lambda i: layout.pipeline_root_prefix(i["conn_prefix"], i["pipeline_prefix"]),
    "table_prefix": lambda i: layout.table_prefix(_table(i)),
    "sidecar_table_prefix": lambda i: layout.sidecar_table_prefix(_table(i)),
    "load_key": lambda i: layout.load_key(_table(i), i["dt"], i["load_seq"]),
    "cdc_key": lambda i: layout.cdc_key(_table(i), i["ts_ms"], i["partition"], i["first_offset"]),
    "manifest_key": lambda i: layout.manifest_key(_table(i), i["dt"]),
    "success_key": lambda i: layout.success_key(_table(i), i["dt"]),
}

_V1_SECTIONS = {
    "batch_part_key",
    "batch_manifest_key",
    "batch_success_key",
    "table_prefix",
    "cdc_object_key",
    "cdc_pipeline_root_prefix",
}


def _golden() -> dict:
    return json.loads(_GOLDEN.read_text(encoding="utf-8"))


def _v2_cases():
    for section, cases in _golden()["v2"].items():
        for case in cases:
            yield pytest.param(section, case, id=f"{section}: {case['name']}")


def test_golden_sections_match_every_port():
    golden = _golden()
    assert set(golden["v2"]) == set(_RUNNERS)
    assert set(golden["v1"]) == _V1_SECTIONS
    for block in ("v1", "v2"):
        for section, cases in golden[block].items():
            assert cases, f"{block}.{section} has no cases"
    assert sum(len(c) for c in golden["v2"].values()) >= 40


@pytest.mark.parametrize("section,case", list(_v2_cases()))
def test_v2_layout_matches_golden(section, case):
    inp = case["in"]
    unknown = set(inp) - _TABLE_FIELDS - _EXTRA_FIELDS
    assert not unknown, f"unknown input fields {unknown}"
    assert ("want" in case) != ("error" in case), "a case needs exactly one of want / error"
    if "error" in case:
        with pytest.raises(layout.ObjectLayoutError) as exc:
            _RUNNERS[section](inp)
        assert exc.value.code == case["error"]
    else:
        assert _RUNNERS[section](inp) == case["want"]
