"""RED regression for DEFECT d06_auth_prefix — auth-prefix must match auth type.

The single-auth `_get_headers` path in the REST connector template
(`connector.py.j2`) must stamp an auth-value prefix that matches the auth type:

  * basic   -> "Basic "  (never "Bearer")
  * bearer  -> "Bearer " (when header_prefix defaults / is None)
  * api_key -> the explicit header_prefix, and NO prefix when none is declared.

The bug: `AuthConfig.header_prefix` schema-defaults to the literal string
"Bearer" for *every* auth type. The api_key branch renders the prefix as
``"{{ spec.auth.header_prefix or '' }}"`` which evaluates ``"Bearer" or ''``
-> ``"Bearer"`` and produces ``<header>: Bearer <api_key>`` — a bogus Bearer
prefix on an API-key credential. The bearer branch already guards this with
``is not none else 'Bearer'``; the api_key branch does not.

This renders fully in-process (no network, no persistence to
shared/mcp-connectors, no docker): build a ``ConnectorSpec`` and render it
through ``ConnectorBuilder().build(spec)``, then assert on the returned code
string — the same recipe as
``llm-service/src/agents/tool_generator/tests/test_builder.py``.
"""

from __future__ import annotations

import pytest

# Import style mirrors the generator tests. The repo conftest.py puts both
# ``llm-service/src`` and the generator package root on sys.path, so tolerate
# either the fully-qualified ``agents.tool_generator.*`` root or the bare
# ``agents.*`` / ``generator.*`` / ``schemas.*`` roots.
try:  # pragma: no cover - import shim
    from agents.tool_generator.schemas.spec import (
        ConnectorSpec,
        AuthConfig,
        AuthType,
        ConnectorCategory,
        ConfigField,
        ResourceConfig,
        create_api_operations,
    )
    from agents.tool_generator.generator.builder import ConnectorBuilder
except ImportError:  # pragma: no cover - import shim
    from schemas.spec import (  # type: ignore
        ConnectorSpec,
        AuthConfig,
        AuthType,
        ConnectorCategory,
        ConfigField,
        ResourceConfig,
        create_api_operations,
    )
    from generator.builder import ConnectorBuilder  # type: ignore


def _api_spec(name: str, auth: AuthConfig) -> ConnectorSpec:
    """Minimal REST/api_saas spec that renders through ConnectorBuilder.

    Shape mirrors the proven ``api_spec`` fixture in test_builder.py; only the
    ``auth`` block varies per case. A single resource + create_api_operations()
    is enough to satisfy ConnectorSpec validation and exercise _get_headers.
    """
    return ConnectorSpec(
        name=name,
        display_name=name.replace("_", " ").title(),
        version="1.0.0",
        description="d06 auth-prefix render fixture",
        category=ConnectorCategory.API_SAAS,
        supports_source=True,
        supports_destination=False,
        base_url="https://api.test.com/v1",
        auth=auth,
        config_fields=[
            ConfigField(name="api_key", required=False, secret=True, env_var="MCP_API_KEY"),
            ConfigField(name="access_token", required=False, secret=True),
            ConfigField(name="username", required=False, secret=True),
            ConfigField(name="password", required=False, secret=True),
        ],
        operations=create_api_operations(),
        resources=[
            ResourceConfig(
                name="items",
                endpoint="/items",
                supports_list=True,
                supports_get=True,
            ),
        ],
    )


def _get_headers_body(code: str) -> str:
    """Return only the rendered ``_get_headers`` method body.

    Isolating the method avoids matching the word "Bearer" that appears in the
    bearer branch's *comment* — we want to assert on the actual prefix
    assignment / emitted header value, not on prose elsewhere in the file.
    """
    marker = "def _get_headers("
    start = code.find(marker)
    assert start != -1, "generated connector has no _get_headers method"
    # Slice to the next method definition after _get_headers.
    next_def = code.find("\n    def ", start + len(marker))
    return code[start:] if next_def == -1 else code[start:next_def]


def _render(spec: ConnectorSpec) -> str:
    result = ConnectorBuilder().build(spec)
    assert result.code, "ConnectorBuilder returned empty code"
    return result.code


# ---------------------------------------------------------------------------
# RED case: api_key with no explicit prefix must NOT emit a Bearer prefix.
# ---------------------------------------------------------------------------
def test_api_key_no_prefix_does_not_emit_bogus_bearer():
    """api_key auth, header_prefix left at its (shared) default.

    AuthConfig.header_prefix defaults to "Bearer" for every auth type, so a
    plain api_key spec currently renders ``prefix = "Bearer".strip()`` and
    stamps ``X-API-Key: Bearer <key>``. That bogus Bearer prefix must be gone:
    an api_key with no declared prefix carries no prefix.
    """
    spec = _api_spec(
        "d06_api_key_no_prefix",
        AuthConfig(
            type=AuthType.API_KEY,
            header_name="X-API-Key",
            # header_prefix intentionally omitted -> schema default "Bearer".
            env_var="MCP_API_KEY",
        ),
    )
    body = _get_headers_body(_render(spec))

    # The api_key branch is the one that was rendered (single auth method).
    assert 'header_name = "X-API-Key"' in body, (
        "expected the api_key single-auth branch to render; got:\n" + body
    )
    # FAILS today: currently renders `prefix = "Bearer".strip()`.
    assert 'prefix = "Bearer".strip()' not in body, (
        "api_key auth with no explicit header_prefix rendered a bogus "
        "'Bearer' prefix — an API key must not be stamped with Bearer:\n" + body
    )
    # After the fix the api_key prefix is empty.
    assert 'prefix = "".strip()' in body, (
        "api_key branch should render an empty prefix when none is declared:\n"
        + body
    )


def test_api_key_explicit_prefix_is_preserved():
    """An explicitly declared non-Bearer prefix must survive rendering."""
    spec = _api_spec(
        "d06_api_key_token_prefix",
        AuthConfig(
            type=AuthType.API_KEY,
            header_name="Authorization",
            header_prefix="Token",
            env_var="MCP_API_KEY",
        ),
    )
    body = _get_headers_body(_render(spec))
    assert 'prefix = "Token".strip()' in body, (
        "explicit api_key header_prefix 'Token' was not preserved:\n" + body
    )


def test_api_key_empty_prefix_is_preserved():
    """Shopify-style header with an explicit empty prefix stays empty."""
    spec = _api_spec(
        "d06_api_key_empty_prefix",
        AuthConfig(
            type=AuthType.API_KEY,
            header_name="X-Shopify-Access-Token",
            header_prefix="",  # explicit no-prefix
            env_var="SHOPIFY_TOKEN",
        ),
    )
    body = _get_headers_body(_render(spec))
    assert 'prefix = "".strip()' in body, (
        "explicit empty header_prefix must render an empty prefix:\n" + body
    )
    # Assert on the EXECUTABLE auth code, not comment text: the fix's explanatory
    # comment legitimately mentions the word 'Bearer' (documenting why the shared
    # default is treated as unset). Strip comment lines before the check so this
    # guards behaviour (no 'Bearer' prefix is applied), not prose.
    executable = "\n".join(
        l for l in body.splitlines() if not l.lstrip().startswith("#")
    )
    assert "Bearer" not in executable, (
        "no Bearer prefix should appear in the executable auth code for an "
        "explicit empty-prefix api_key:\n" + body
    )


# ---------------------------------------------------------------------------
# Guard cases: bearer keeps Bearer; basic keeps Basic (regression guards).
# ---------------------------------------------------------------------------
def test_bearer_still_emits_bearer_prefix():
    spec = _api_spec(
        "d06_bearer",
        AuthConfig(
            type=AuthType.BEARER,
            header_name="Authorization",
            header_prefix="Bearer",
            env_var="MCP_API_KEY",
        ),
    )
    body = _get_headers_body(_render(spec))
    assert 'prefix = "Bearer".strip()' in body, (
        "bearer auth must still render a 'Bearer' prefix:\n" + body
    )


def test_basic_emits_basic_not_bearer():
    spec = _api_spec(
        "d06_basic",
        AuthConfig(
            type=AuthType.BASIC,
            username_env="MCP_BASIC_USER",
            password_env="MCP_BASIC_PASSWORD",
        ),
    )
    body = _get_headers_body(_render(spec))
    assert 'f"Basic {credentials}"' in body, (
        "basic auth must emit a 'Basic <credentials>' Authorization header:\n"
        + body
    )
    assert 'f"Bearer {credentials}"' not in body, (
        "basic auth must never emit a 'Bearer' Authorization header:\n" + body
    )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
