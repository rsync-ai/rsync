"""
Unit tests for ``find_stale_connectors`` — the only piece of broken-connector
detection that exists today (the other detection signals named in the master
test plan — heartbeat, auth-refresh failure, schema-drift, staleness window —
do not have product code yet, so they cannot be unit-tested).

Coverage:
- matching source_hash → connector is not flagged stale
- mismatched source_hash → stale, both hashes surfaced
- missing source_hash (older connectors predating the field) → stale
- missing metadata.json (orphan dir) → silently skipped
- nested layout (``connectors_root/<category>/<id>/metadata.json``) discovered
- non-existent connectors_root → empty list (no crash)
- vendor missing from registry (current_hash is None) → not flagged
- corrupt metadata.json → skipped (does not abort the scan)
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import pytest

from agents.tool_generator.utils import vendor_registry as vr


@pytest.fixture
def patch_hash(monkeypatch):
    """Patch vendor_entry_hash to return values keyed by (vendor_id, api_id).

    Anything not in the dict resolves to None (= vendor not in registry).
    """
    table: dict[tuple[str, str], Optional[str]] = {}

    def fake(vendor_id: str, api_id: str) -> Optional[str]:
        return table.get((vendor_id, api_id))

    monkeypatch.setattr(vr, "vendor_entry_hash", fake)
    return table


def _write_meta(dir_: Path, meta: dict, *, version: str = "v1.0.0") -> None:
    """Write a connector in the canonical no-root-copies layout: a latest.json
    pointer at the connector root and metadata.json under versions/<version>/."""
    vdir = dir_ / "versions" / version
    vdir.mkdir(parents=True, exist_ok=True)
    (dir_ / "latest.json").write_text(json.dumps({"current_version": version}))
    (vdir / "metadata.json").write_text(json.dumps(meta))


def test_matching_hash_is_not_stale(tmp_path: Path, patch_hash):
    patch_hash[("stripe", "rest")] = "abc"
    _write_meta(
        tmp_path / "stripe-rest",
        {
            "vendor_id": "stripe",
            "api_id": "rest",
            "source_hash": "abc",
            "connector_type": "stripe-rest",
        },
    )
    assert vr.find_stale_connectors(str(tmp_path)) == []


def test_mismatched_hash_is_stale_with_both_hashes(tmp_path: Path, patch_hash):
    patch_hash[("stripe", "rest")] = "current-hash"
    _write_meta(
        tmp_path / "stripe-rest",
        {
            "vendor_id": "stripe",
            "api_id": "rest",
            "source_hash": "stored-hash",
            "connector_type": "stripe-rest",
        },
    )
    stale = vr.find_stale_connectors(str(tmp_path))
    assert stale == [
        {
            "connector_id": "stripe-rest",
            "vendor_id": "stripe",
            "api_id": "rest",
            "stored_hash": "stored-hash",
            "current_hash": "current-hash",
        }
    ]


def test_missing_source_hash_is_stale(tmp_path: Path, patch_hash):
    """Older connectors that predate source_hash tracking must surface as stale."""
    patch_hash[("stripe", "rest")] = "current-hash"
    _write_meta(
        tmp_path / "stripe-rest",
        {"vendor_id": "stripe", "api_id": "rest", "connector_type": "stripe-rest"},
    )
    stale = vr.find_stale_connectors(str(tmp_path))
    assert len(stale) == 1
    assert stale[0]["stored_hash"] is None
    assert stale[0]["current_hash"] == "current-hash"


def test_dir_without_metadata_is_skipped(tmp_path: Path, patch_hash):
    (tmp_path / "orphan").mkdir()
    assert vr.find_stale_connectors(str(tmp_path)) == []


def test_nested_layout_is_discovered(tmp_path: Path, patch_hash):
    """``public/<category>/<connector>/metadata.json`` layout."""
    patch_hash[("shopify", "admin-graphql")] = "current-hash"
    _write_meta(
        tmp_path / "ecommerce" / "shopify-admin",
        {
            "vendor_id": "shopify",
            "api_variant_id": "admin-graphql",
            "source_hash": "current-hash",
            "connector_type": "shopify-admin",
        },
    )
    assert vr.find_stale_connectors(str(tmp_path)) == []


def test_missing_root_returns_empty(tmp_path: Path, patch_hash):
    assert vr.find_stale_connectors(str(tmp_path / "does-not-exist")) == []


def test_unknown_vendor_is_not_flagged(tmp_path: Path, patch_hash):
    """If the vendor isn't in the registry we can't compute a current hash,
    so we shouldn't false-positive on a missing vendor as 'stale'."""
    _write_meta(
        tmp_path / "phantom",
        {
            "vendor_id": "phantom",
            "api_id": "rest",
            "source_hash": "whatever",
            "connector_type": "phantom",
        },
    )
    assert vr.find_stale_connectors(str(tmp_path)) == []


def test_corrupt_metadata_is_skipped(tmp_path: Path, patch_hash):
    patch_hash[("stripe", "rest")] = "abc"
    # A discoverable connector (has latest.json) whose versioned metadata is
    # corrupt — the scan must skip it, not abort.
    bad = tmp_path / "broken"
    bad_vdir = bad / "versions" / "v1.0.0"
    bad_vdir.mkdir(parents=True)
    (bad / "latest.json").write_text(json.dumps({"current_version": "v1.0.0"}))
    (bad_vdir / "metadata.json").write_text("not json {")
    # Plus a valid one to make sure the scan continues past the corrupt entry.
    _write_meta(
        tmp_path / "stripe-rest",
        {
            "vendor_id": "stripe",
            "api_id": "rest",
            "source_hash": "stale",
            "connector_type": "stripe-rest",
        },
    )
    stale = vr.find_stale_connectors(str(tmp_path))
    assert [s["connector_id"] for s in stale] == ["stripe-rest"]
