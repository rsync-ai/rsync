"""
Unit tests for ``scripts/regenerate_connectors.py``.

The script is the only "regenerate this connector" path that exists today.
There is no per-connector regenerate API endpoint and no UI button yet, so
the script's HTTP-call logic and small filter helpers are the surface we
can actually pin down with tests.

Coverage:
- regenerate_connector: 200 + success=True → True
- regenerate_connector: 200 + success=False surfaces error_message
- regenerate_connector: non-2xx → False (and does not raise)
- regenerate_connector: network exception → False (and does not raise)
- regenerate_connector: dry_run short-circuits before any HTTP call
- _load_connector_category: returns category from metadata.json
- _load_connector_category: returns None when metadata.json missing
- _load_connector_category: returns None on corrupt metadata.json
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

import pytest


# --------------------------------------------------------------------------- #
# Load the script as a module. It lives under /scripts so it isn't installed
# as a package; importlib lets us pull it in without restructuring the repo.
# --------------------------------------------------------------------------- #


_REPO_ROOT = Path(__file__).resolve().parents[3]
_SCRIPT_PATH = _REPO_ROOT / "scripts" / "regenerate_connectors.py"


@pytest.fixture(scope="module")
def regen_module():
    spec = importlib.util.spec_from_file_location(
        "rsync_regenerate_connectors", _SCRIPT_PATH
    )
    assert spec and spec.loader, f"cannot load spec for {_SCRIPT_PATH}"
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


# --------------------------------------------------------------------------- #
# Fakes
# --------------------------------------------------------------------------- #


class _FakeResp:
    def __init__(self, status: int, body: dict[str, Any] | None = None, text: str = "") -> None:
        self.status_code = status
        self._body = body or {}
        self.text = text or json.dumps(self._body)

    def json(self) -> dict[str, Any]:
        return self._body


# --------------------------------------------------------------------------- #
# regenerate_connector
# --------------------------------------------------------------------------- #


def test_regenerate_success(monkeypatch, regen_module):
    calls: list[dict] = []

    def fake_post(url, *, json: dict, timeout: int):
        calls.append({"url": url, "json": json, "timeout": timeout})
        return _FakeResp(200, {"success": True})

    monkeypatch.setattr(regen_module.requests, "post", fake_post)

    assert regen_module.regenerate_connector("stripe-rest") is True
    assert len(calls) == 1
    assert calls[0]["url"].endswith("/v1/generate")
    assert calls[0]["json"]["api_name"] == "stripe-rest"
    assert calls[0]["json"]["enable_chaos"] is False  # bulk regen defaults off


def test_regenerate_passes_through_success_false(monkeypatch, regen_module, capsys):
    monkeypatch.setattr(
        regen_module.requests,
        "post",
        lambda *_a, **_kw: _FakeResp(200, {"success": False, "error_message": "boom"}),
    )

    assert regen_module.regenerate_connector("stripe-rest") is False
    err = capsys.readouterr().out
    assert "boom" in err


def test_regenerate_non_2xx_returns_false(monkeypatch, regen_module, capsys):
    monkeypatch.setattr(
        regen_module.requests,
        "post",
        lambda *_a, **_kw: _FakeResp(500, text="service unavailable"),
    )
    assert regen_module.regenerate_connector("stripe-rest") is False
    assert "500" in capsys.readouterr().out


def test_regenerate_network_error_returns_false(monkeypatch, regen_module, capsys):
    def explode(*_a, **_kw):
        raise regen_module.requests.exceptions.ConnectionError("boom")

    monkeypatch.setattr(regen_module.requests, "post", explode)
    # Must not raise — bulk regen continues past one bad connector.
    assert regen_module.regenerate_connector("stripe-rest") is False
    assert "Error" in capsys.readouterr().out


def test_dry_run_skips_http(monkeypatch, regen_module):
    """dry_run must short-circuit before any HTTP call, even if the URL would fail."""

    def must_not_be_called(*_a, **_kw):  # pragma: no cover — defensive
        raise AssertionError("HTTP must not be called in dry_run")

    monkeypatch.setattr(regen_module.requests, "post", must_not_be_called)
    assert regen_module.regenerate_connector("stripe-rest", dry_run=True) is True


# --------------------------------------------------------------------------- #
# _load_connector_category
# --------------------------------------------------------------------------- #


def test_load_category_from_metadata(tmp_path: Path, regen_module):
    (tmp_path / "metadata.json").write_text(json.dumps({"category": "api_saas"}))
    assert regen_module._load_connector_category(tmp_path) == "api_saas"


def test_load_category_missing_metadata_returns_none(tmp_path: Path, regen_module):
    assert regen_module._load_connector_category(tmp_path) is None


def test_load_category_corrupt_metadata_returns_none(tmp_path: Path, regen_module):
    (tmp_path / "metadata.json").write_text("not json {")
    assert regen_module._load_connector_category(tmp_path) is None
