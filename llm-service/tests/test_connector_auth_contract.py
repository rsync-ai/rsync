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


# ---------------------------------------------------------------------------
# The connection form's Save/Test gate agrees with the SERVER's gate, and both
# read ``config_schema.required``. That only protects anyone if `required` is
# honest, so this section makes a dishonest one un-mergeable.
#
# Server gate:  backend-orchestrator/internal/mcp/server_manager.go
#               missingRequiredConfig() — key presence over `required_config`,
#               honouring `config_aliases`.
# Form gate:    frontend/src/components/connectors/GenericConnectorForm.tsx
#               authIncomplete() — the chosen method's credential fields that
#               `configuration_schema.required` names must be non-blank.
#
# Consequence: a connector whose selected auth method has NO credential field in
# `required` can be saved with every credential blank. For mongodb (an
# unauthenticated deployment), gcs/bigquery (Application Default Credentials)
# and azure-blob (anonymous / emulator) that is correct and intended. For a
# vendor that always needs a secret it is a broken connection the user only
# discovers at run time — which is exactly what stripe shipped: a bearer method
# naming four credential keys and a `required` list of [].
#
# The two cases are indistinguishable from the data, so the honest one has to
# say so: ``"credentials_optional": true`` on the auth method. Default false
# means a new or generated connector fails this gate until someone decides which
# case it is.


def _split_method_credential_keys(
    method: dict, schema_keys: set[str]
) -> list[str]:
    """The credential fields the connection form renders for ``method``.

    A line-for-line mirror of ``splitMethodCredentialKeys`` in
    frontend/src/lib/types/mcp-connector.ts (it returns {fields, aliases}; only
    ``fields`` — the gated set — matters here). Keep the two in lockstep: this
    gate is only meaningful while it models the field set the form actually
    gates on.
    """
    keys = [k for k in (method.get("config_keys") or []) if isinstance(k, str)]
    kind = method.get("method")
    if kind in ("oauth2", "oauth"):
        return []
    if kind == "basic":
        return list(keys)
    distinct = [k for k in keys if k.lower() in schema_keys]
    if distinct:
        return distinct
    return keys[:1]


def _ungated_auth_methods(meta: dict) -> list[str]:
    """Auth methods that gate on nothing and do not admit it.

    Returns one human-readable complaint per offending method; empty means the
    connector's declared contract is honest either way.
    """
    schema = meta.get("config_schema") or meta.get("configuration_schema") or {}
    schema_keys = {k.lower() for k in (schema.get("properties") or {})}
    required = {k.lower() for k in (schema.get("required") or [])}
    # An alias of a required field satisfies the server gate, so it satisfies
    # this one: missingRequiredConfig() accepts `config[alias]` for `required`.
    for req, alias_list in (meta.get("config_aliases") or {}).items():
        if req.lower() in required:
            required.update(a.lower() for a in alias_list if isinstance(a, str))

    complaints: list[str] = []
    for i, method in enumerate(meta.get("supported_auth_methods") or []):
        if not isinstance(method, dict) or method.get("method") in ("oauth2", "oauth"):
            continue
        fields = _split_method_credential_keys(method, schema_keys)
        if any(f.lower() in required for f in fields):
            continue
        if method.get("credentials_optional") is True:
            continue
        complaints.append(
            f"supported_auth_methods[{i}] (method={method.get('method')!r}, "
            f"config_keys={method.get('config_keys')!r}): none of its credential "
            f"fields {fields} appears in config_schema.required, and it does not "
            f'declare "credentials_optional": true. A connection saves with every '
            f"credential blank and fails at run time. Either add the field the "
            f"vendor actually needs to `required` (+ `config_aliases` for the "
            f"other spellings), or declare credentials_optional if this method "
            f"genuinely works with no credential."
        )
    return complaints


def _count_gated_methods() -> int:
    """How many non-oauth methods the gate below actually examines."""
    n = 0
    for _, meta_path in _CONNECTORS:
        meta = json.loads(meta_path.read_text())
        for method in meta.get("supported_auth_methods") or []:
            if isinstance(method, dict) and method.get("method") not in ("oauth2", "oauth"):
                n += 1
    return n


def test_the_ungated_auth_check_has_a_real_corpus_to_check():
    """An empty corpus would make every assertion below pass vacuously."""
    n = _count_gated_methods()
    assert n >= 10, (
        f"only {n} non-oauth auth methods discovered across {len(_CONNECTORS)} "
        f"connectors — the ungated-auth gate would be near-vacuous"
    )


def test_the_ungated_auth_check_rejects_a_dishonest_contract():
    """Control: the checker must FAIL the shape it exists to catch.

    Without this, a refactor that makes ``_ungated_auth_methods`` always return
    [] would turn the whole gate green while catching nothing.
    """
    dishonest = {
        "supported_auth_methods": [
            {
                "method": "bearer",
                "config_keys": ["access_token", "token", "secret_key"],
            }
        ],
        "config_schema": {
            "properties": {"base_url": {}, "access_token": {}},
            "required": [],
        },
    }
    assert _ungated_auth_methods(dishonest), (
        "the checker accepted a bearer method whose credential is in neither "
        "`required` nor `credentials_optional` — it is not checking anything"
    )

    # …and must accept both honest shapes, so it is not merely always-failing.
    by_required = json.loads(json.dumps(dishonest))
    by_required["config_schema"]["required"] = ["access_token"]
    assert _ungated_auth_methods(by_required) == []

    by_alias = json.loads(json.dumps(dishonest))
    by_alias["config_schema"]["required"] = ["secret_key"]
    by_alias["config_aliases"] = {"secret_key": ["access_token"]}
    assert _ungated_auth_methods(by_alias) == []

    by_marker = json.loads(json.dumps(dishonest))
    by_marker["supported_auth_methods"][0]["credentials_optional"] = True
    assert _ungated_auth_methods(by_marker) == []


@pytest.mark.parametrize("connector_id,meta_path", _CONNECTORS, ids=_IDS)
def test_every_auth_method_gates_on_something_or_says_it_does_not(
    connector_id: str, meta_path: Path
):
    """HARD GATE: a credential field in `required`, or `credentials_optional: true`.

    Applies to every connector on disk, including ones the generator writes —
    a generated connector lands here as a checked-in metadata.json like any
    other, so this is the gate future connectors have to pass too.
    """
    complaints = _ungated_auth_methods(json.loads(meta_path.read_text()))
    assert not complaints, f"{connector_id}:\n  " + "\n  ".join(complaints)


@pytest.mark.parametrize("connector_id,meta_path", _CONNECTORS, ids=_IDS)
def test_credentials_optional_is_a_boolean_when_present(
    connector_id: str, meta_path: Path
):
    """A string "false" is truthy in JS and would silently disarm the form gate."""
    meta = json.loads(meta_path.read_text())
    for i, method in enumerate(meta.get("supported_auth_methods") or []):
        if not isinstance(method, dict) or "credentials_optional" not in method:
            continue
        assert isinstance(method["credentials_optional"], bool), (
            f"{connector_id}.supported_auth_methods[{i}]: credentials_optional="
            f"{method['credentials_optional']!r} must be a JSON boolean"
        )
