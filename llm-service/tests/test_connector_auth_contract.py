"""Canonical auth-contract gate for every checked-in public connector.

This is the hard CI enforcement of the canonical auth metadata contract
(see the "Canonical auth contract" entry in ``INVENTORY.md``). The connection
modal reads ``auth_type`` + ``oauth_provider`` + ``supported_auth_methods``
to decide which auth UI to render; if a connector declares it needs a
credential (``auth_type != "none"``) but ships no ``supported_auth_methods``,
the modal falls back to brittle per-connector inference — the exact path that
produced #290 (aws-s3) and #291 (github-rest). This test makes that state
un-mergeable.

Discovery uses the canonical resolver so we read the version that actually
runs (``versions/<current_version>/metadata.json``), never a stale root copy.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.utils.connector_paths import iter_current_artifact

# Repo root is two levels above this file: <repo>/llm-service/tests/<this>.
_PUBLIC_CONNECTORS_ROOT = (
    Path(__file__).resolve().parents[2] / "shared" / "mcp-connectors" / "public"
)
_PROVIDERS_JSON = (
    Path(__file__).resolve().parents[2]
    / "shared"
    / "mcp-connectors"
    / "oauth"
    / "providers.json"
)


def _registered_provider_ids() -> set[str]:
    """Top-level provider ids registered in providers.json (the OAuth runtime source).

    Returns an empty set if the file is unreadable; the membership assertion below
    only tightens when the registry is present, so a missing file degrades to the
    weaker "provider id is non-empty" check rather than failing spuriously.
    """
    try:
        data = json.loads(_PROVIDERS_JSON.read_text())
    except Exception:  # pragma: no cover - exercised only on a broken checkout
        return set()
    # providers.json is a flat object keyed by provider id; tolerate a future
    # {"providers": {...}} envelope without breaking the gate.
    if isinstance(data, dict) and isinstance(data.get("providers"), dict):
        data = data["providers"]
    return set(data.keys()) if isinstance(data, dict) else set()


_REGISTERED_PROVIDERS = _registered_provider_ids()

# §2 contract: the closed set of auth classifiers a connector may declare.
VALID_AUTH_TYPES = {
    "none",
    "api_key",
    "api_key_query",
    "bearer",
    "basic",
    "oauth2",
    "custom_header",
}
# A supported_auth_methods entry's `method` uses the same vocabulary minus "none".
VALID_METHODS = VALID_AUTH_TYPES - {"none"}


def _discover_connectors() -> list[tuple[str, Path]]:
    """Return (connector_id, metadata_path) for every public connector on disk."""
    found: list[tuple[str, Path]] = []
    for meta_path in iter_current_artifact(_PUBLIC_CONNECTORS_ROOT, "metadata.json"):
        try:
            data = json.loads(meta_path.read_text())
        except Exception:  # pragma: no cover - surfaced by the validity test below
            found.append((str(meta_path), meta_path))
            continue
        found.append((data.get("id") or meta_path.parent.name, meta_path))
    return sorted(found, key=lambda t: t[0])


_CONNECTORS = _discover_connectors()
_IDS = [cid for cid, _ in _CONNECTORS]


def test_at_least_one_connector_discovered():
    """Guard against a broken root path silently making the whole gate a no-op."""
    assert _CONNECTORS, f"no connectors discovered under {_PUBLIC_CONNECTORS_ROOT}"


@pytest.mark.parametrize("connector_id,meta_path", _CONNECTORS, ids=_IDS)
def test_connector_metadata_is_valid_json(connector_id: str, meta_path: Path):
    json.loads(meta_path.read_text())  # raises -> fails with the offending file


@pytest.mark.parametrize("connector_id,meta_path", _CONNECTORS, ids=_IDS)
def test_auth_type_is_in_the_canonical_enum(connector_id: str, meta_path: Path):
    meta = json.loads(meta_path.read_text())
    auth_type = meta.get("auth_type")
    assert auth_type in VALID_AUTH_TYPES, (
        f"{connector_id}: auth_type={auth_type!r} not in canonical enum "
        f"{sorted(VALID_AUTH_TYPES)}"
    )


@pytest.mark.parametrize("connector_id,meta_path", _CONNECTORS, ids=_IDS)
def test_credentialed_connector_declares_supported_auth_methods(
    connector_id: str, meta_path: Path
):
    """HARD GATE: ``auth_type != "none"`` ⟹ ``supported_auth_methods`` non-empty.

    This is the mandatory rule (user decision 2026-06-20): every credentialed
    connector must declare its auth methods so the modal never falls back to
    per-connector inference.
    """
    meta = json.loads(meta_path.read_text())
    auth_type = meta.get("auth_type")
    methods = meta.get("supported_auth_methods")
    if auth_type == "none":
        return  # public APIs declare no methods (e.g. countries-gql)
    assert isinstance(methods, list) and methods, (
        f"{connector_id}: auth_type={auth_type!r} requires a non-empty "
        f"supported_auth_methods array (got {methods!r}). Backfill it per the "
        f"canonical auth contract."
    )


@pytest.mark.parametrize("connector_id,meta_path", _CONNECTORS, ids=_IDS)
def test_supported_auth_method_entries_are_well_formed(
    connector_id: str, meta_path: Path
):
    meta = json.loads(meta_path.read_text())
    for i, method in enumerate(meta.get("supported_auth_methods", []) or []):
        where = f"{connector_id}.supported_auth_methods[{i}]"
        assert isinstance(method, dict), f"{where}: not an object"
        m = method.get("method")
        assert m in VALID_METHODS, (
            f"{where}: method={m!r} not in {sorted(VALID_METHODS)}"
        )
        assert isinstance(method.get("config_keys"), list), (
            f"{where}: config_keys must be a list"
        )


@pytest.mark.parametrize("connector_id,meta_path", _CONNECTORS, ids=_IDS)
def test_oauth2_methods_resolve_a_registered_provider(
    connector_id: str, meta_path: Path
):
    """An oauth2 method must resolve a provider key (method-level or connector-level).

    Without it the modal would render a free-text paste-token box and no Connect
    button (the pipedrive footgun). Provider precedence mirrors the frontend:
    ``method.oauth_provider ?? connector.oauth_provider``.
    """
    meta = json.loads(meta_path.read_text())
    connector_provider = meta.get("oauth_provider")
    for i, method in enumerate(meta.get("supported_auth_methods", []) or []):
        if method.get("method") != "oauth2":
            continue
        provider = method.get("oauth_provider") or connector_provider
        assert provider, (
            f"{connector_id}.supported_auth_methods[{i}]: oauth2 method has no "
            f"resolvable oauth_provider (method-level or connector-level). Link it "
            f"to a registered provider in providers.json."
        )
        # Tighter than "non-empty": the id must actually exist in providers.json,
        # else the OAuth runtime 400s at /v1/oauth/authorize (the pipedrive footgun
        # was a *declared* provider with no runtime entry). Skipped only when the
        # registry file itself is unreadable (degrades to the non-empty check above).
        if _REGISTERED_PROVIDERS:
            assert provider in _REGISTERED_PROVIDERS, (
                f"{connector_id}.supported_auth_methods[{i}]: oauth_provider="
                f"{provider!r} is not registered in providers.json "
                f"({sorted(_REGISTERED_PROVIDERS)}). The OAuth Connect flow would "
                f"fail at runtime. Register the provider or fix the id."
            )
