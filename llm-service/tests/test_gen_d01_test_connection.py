"""RED gate for d01_test_connection — generated test_connection() must probe the
connector's own listable resource endpoint FIRST (with a minimal page), and use
the whoami endpoints only as a last resort.

Today the generator (``templates/connector.py.j2``) hard-codes a ``whoami``
candidate list (``/users/me``, ``/me``, ``/whoami``, ``/account`` …) and builds
the probe order as ``whoami_candidates`` FIRST, then the connector's own
spec-declared resource endpoints — and issues every probe with no page param.
For real vendors none of those whoami paths exist, so test_connection never
exercises the connector's real list+auth path. This test renders a REST
connector from a contract with a single listable ``/contacts`` resource fully
in-process (no network, no persistence, no docker) and asserts:

  1. the connector's list endpoint (``/contacts``) is a probe candidate, and
  2. the resource probes are attempted BEFORE the whoami probes, and
  3. a resource probe carries a minimal page (``limit=1``) rather than the bare
     no-params request that ships today.

Mirrors the in-process render recipe of
``src/agents/tool_generator/tests/test_end_to_end_review.py``: build a
``GenerationContract`` -> ``architect_rest.build_rest_spec`` ->
``generator.builder.ConnectorBuilder().build(spec)`` -> assert on the returned
generated ``connector.py`` source string.
"""

from __future__ import annotations

import os
import sys

import pytest

# Make the tool_generator package importable under whichever rootdir pytest uses.
# (a) rootdir=llm-service adds src/ to sys.path via llm-service/conftest.py, so
#     ``from schemas.contract import ...`` and ``from agents.architect_rest ...``
#     resolve; (b) to be robust when this file is collected on its own we also
#     put the tool_generator dir itself on sys.path, exactly like
#     test_end_to_end_review.py does.
_TOOLGEN = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "src",
    "agents",
    "tool_generator",
)
if os.path.isdir(_TOOLGEN) and _TOOLGEN not in sys.path:
    sys.path.insert(0, _TOOLGEN)

# Import the contract schema + REST architect + builder, tolerating both the
# ``agents.*`` (rootdir=tool_generator/llm-service) and
# ``agents.tool_generator.agents.*`` (rootdir=src) conventions used across the
# existing generator test suite.
try:  # pragma: no cover - import-context shim
    from schemas.contract import Dimension, Fact, FactSource, empty_contract
    from agents.architect_rest import build_rest_spec
    from generator.builder import ConnectorBuilder
except ImportError:  # pragma: no cover - import-context shim
    from agents.tool_generator.schemas.contract import (  # type: ignore
        Dimension,
        Fact,
        FactSource,
        empty_contract,
    )
    from agents.tool_generator.agents.architect_rest import build_rest_spec  # type: ignore
    from agents.tool_generator.generator.builder import ConnectorBuilder  # type: ignore


LIST_ENDPOINT = "/contacts"


def _listable_rest_contract():
    """Minimal REST contract: one listable, path-param-free ``/contacts`` resource.

    ``vendor_rest_resources`` is the highest-priority resource source in
    ``architect_rest._build_resources``; each entry becomes a ResourceConfig with
    ``supports_list=True`` and no path_params, i.e. exactly the shape that
    ``test_connection`` is supposed to prefer as its reachability probe.
    """
    c = empty_contract("d01-session", "acme")
    c.set_fact(
        Fact(
            dimension=Dimension.BASE_URL,
            value="https://api.acme.example/v1",
            confidence=1.0,
            source=FactSource.VENDOR_YAML,
        )
    )
    c.metadata["vendor_rest_resources"] = [
        {"name": "contacts", "endpoint": LIST_ENDPOINT, "id_field": "id"}
    ]
    return c


def _render_rest_connector() -> str:
    spec = build_rest_spec(_listable_rest_contract())
    # include_dockerfile=False: we only inspect connector.py; a Dockerfile would
    # require additional spec fields and is irrelevant to this gate.
    result = ConnectorBuilder().build(spec, include_dockerfile=False)
    assert result.code, (
        "generator returned empty connector.py — build failed before we could "
        f"assert on test_connection(). validation_errors={getattr(result, 'validation_errors', None)}"
    )
    return result.code


@pytest.fixture(scope="module")
def connector_code() -> str:
    return _render_rest_connector()


def test_list_endpoint_is_a_probe_candidate(connector_code: str):
    """Sanity: the connector's own list endpoint must appear as a probe target."""
    assert LIST_ENDPOINT in connector_code, (
        f"generated connector never references its list endpoint {LIST_ENDPOINT!r} "
        "in test_connection — the resource-first probe cannot work"
    )


def test_resource_probes_run_before_whoami(connector_code: str):
    """The connector's OWN listable resource must be probed BEFORE the generic
    whoami endpoints — otherwise test_connection depends on /whoami|/users/me
    existing, which they do not for most vendors.

    RED today: the template builds ``[(ep, False) for ep in whoami_candidates] +
    [(ep, True) for ep in resource_candidates if ep]`` (whoami first).
    """
    resource_probe = "for ep in resource_candidates"
    whoami_probe = "for ep in whoami_candidates"

    res_idx = connector_code.find(resource_probe)
    who_idx = connector_code.find(whoami_probe)

    assert res_idx != -1, "no resource_candidates comprehension found in probe list"
    assert who_idx != -1, "no whoami_candidates comprehension found in probe list"
    assert res_idx < who_idx, (
        "test_connection probes whoami endpoints BEFORE the connector's own "
        "listable resource — it should probe the resource list endpoint first "
        "and fall back to whoami only as a last resort"
    )


def test_resource_probe_uses_a_minimal_page(connector_code: str):
    """A resource reachability probe must request a minimal page (limit=1), not a
    full unbounded list. RED today: the probe is issued as
    ``self._make_request('GET', ep, config)`` with no params at all.
    """
    # The bare, param-less probe request that ships today must be gone.
    assert "self._make_request('GET', ep, config)" not in connector_code, (
        "test_connection issues the resource probe with no page param — it must "
        "request a minimal page (e.g. limit=1) so the probe is cheap and exercises "
        "the real list path"
    )
    # And a minimal-page limit must be present in the probe path.
    assert ('"limit": 1' in connector_code) or ("'limit': 1" in connector_code), (
        "test_connection never sends a minimal page (limit=1) on the resource probe"
    )
