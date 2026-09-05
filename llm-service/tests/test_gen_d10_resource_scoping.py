"""RED gate for DEFECT d10_resource_scoping — resource cap / scoping for huge specs.

Grounded defect (grep-confirmed in freshly-generated artifacts):
  * ``agents/discoverer.py:1350-1355`` caps the raw endpoint list at 500 but
    applies NO resource-level filter.
  * ``agents/architect_rest.py:204-211`` (``_build_resources``) returns
    ``list(kept.values())`` — one ResourceConfig for EVERY GET-bearing grouped
    OpenAPI path, with NO cap, NO ranking, and NO truncation warning.

For huge specs (jira ~200 ops, datadog ~168, asana ~95) this yields 100s of
resources in the generated connector. The generator must instead:
  1. Cap the resource list at a sane default (~40).
  2. Keep the RANKED top resources (prefer list-capable, prefer top-level /
     shallow paths — the syncable collections a user actually wants).
  3. LOG a warning when truncating (no SILENT cap).

Render recipe mirrors ``tests/test_connector_auth_contract.py`` in spirit
(path-bootstrapped, fully in-process): build a minimal ``GenerationContract`` ->
``agents.architect_rest.build_rest_spec`` -> ``generator.builder.ConnectorBuilder``.
NO network, NO persistence to shared/mcp-connectors, NO docker.

This test FAILS on today's output (≈200 resources, no warning) and PASSES once
the architect caps + ranks + warns.
"""

from __future__ import annotations

import logging
import os
import sys

import pytest

# ---------------------------------------------------------------------------
# Bootstrap so ``schemas`` / ``agents`` resolve to the tool_generator package
# (same recipe the in-container tool_generator tests use — see
# src/agents/tool_generator/tests/test_oauth_flow_capture_integration.py). The
# architect module does ``from schemas.contract import ...`` internally, which
# requires the tool_generator dir on sys.path.
# ---------------------------------------------------------------------------
_TOOLGEN = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "src",
        "agents",
        "tool_generator",
    )
)
if _TOOLGEN not in sys.path:
    sys.path.insert(0, _TOOLGEN)

from schemas.contract import (  # noqa: E402
    Dimension,
    Fact,
    FactSource,
    GenerationContract,
)
from agents.architect_rest import build_rest_spec  # noqa: E402
from generator.builder import ConnectorBuilder  # noqa: E402

# The default resource cap the architect must enforce (~40 per the defect spec).
# The assertion is ``<= EXPECTED_CAP`` so a slightly different tuned value still
# passes as long as the list is bounded and sane.
EXPECTED_CAP = 40


def _huge_spec_contract(
    n_keep: int = EXPECTED_CAP,
    n_drop: int = 170,
) -> GenerationContract:
    """A contract carrying ~200 GET-bearing OpenAPI endpoints.

    Two clearly-ranked cohorts so we can assert WHICH resources survive:

      * ``keep_NN``  — top-level shallow list collections ``/keep_NN`` (depth 1).
        These are the syncable collections a ranking step should PREFER.
      * ``drop_NNN`` — deep-nested collections ``/drop_NNN/{id}/items/detail``
        (grouping key ``/drop_NNN/items/detail``, depth 3). Lower-ranked; these
        are what a cap should shed first.

    Total operations ≈ n_keep + n_drop (~210), comfortably past the ~200 the
    defect cites and well past any sane cap.
    """
    endpoints = []
    for i in range(n_keep):
        endpoints.append(
            {
                "path": f"/keep_{i:02d}",
                "method": "GET",
                "operation_id": f"listKeep{i:02d}",
                "description": f"List keep {i:02d}",
                "pagination_hint": {},
                "record_hint": {},
                "header_hint": {},
            }
        )
    for i in range(n_drop):
        endpoints.append(
            {
                "path": f"/drop_{i:03d}/{{id}}/items/detail",
                "method": "GET",
                "operation_id": f"listDrop{i:03d}",
                "description": f"List drop {i:03d}",
                "pagination_hint": {},
                "record_hint": {},
                "header_hint": {},
            }
        )

    contract = GenerationContract(session_id="d10-test", api_name="hugeapi")
    contract.set_fact(
        Fact(
            dimension=Dimension.BASE_URL,
            value="https://api.hugeapi.test/v1",
            confidence=0.95,
            source=FactSource.OPENAPI_SPEC,
            evidence="test fixture",
        )
    )
    contract.set_fact(
        Fact(
            dimension=Dimension.AUTH_TYPE,
            value="bearer",
            confidence=0.95,
            source=FactSource.OPENAPI_SPEC,
            evidence="test fixture",
        )
    )
    contract.set_fact(
        Fact(
            dimension=Dimension.OPERATIONS,
            value=[ep["operation_id"] for ep in endpoints],
            confidence=0.95,
            source=FactSource.OPENAPI_SPEC,
            evidence=f"{len(endpoints)} endpoints",
        )
    )
    contract.metadata["operations_summary"] = {
        "names": [ep["operation_id"] for ep in endpoints],
        "total": len(endpoints),
        "spec_url": "https://api.hugeapi.test/openapi.json",
        "endpoints": endpoints,
    }
    return contract


def test_sanity_fixture_is_actually_huge():
    """Guard: the fixture really does declare ~200 endpoints (else the cap
    assertion below is a vacuous no-op)."""
    contract = _huge_spec_contract()
    endpoints = contract.metadata["operations_summary"]["endpoints"]
    assert len(endpoints) >= 200, (
        f"fixture must declare ~200 endpoints to exercise the cap; got "
        f"{len(endpoints)}"
    )


def test_resource_count_is_capped():
    """~200 operations -> generated resource count <= cap.

    RED today: the architect returns one resource per GET-bearing grouped path
    (~210), so this fails at ~210 > 40.
    """
    contract = _huge_spec_contract()
    spec = build_rest_spec(contract)
    assert len(spec.resources) <= EXPECTED_CAP, (
        f"resource list is uncapped: got {len(spec.resources)} resources for a "
        f"~200-operation spec (expected <= {EXPECTED_CAP}). The architect must "
        f"cap + rank resources instead of emitting one per endpoint."
    )


def test_kept_resources_are_the_ranked_top_ones():
    """After capping, the surviving resources must be the RANKED top set — the
    shallow, list-capable ``keep_*`` collections — not arbitrary deep-nested
    ``drop_*`` ones.

    RED today: without ranking the cap doesn't exist, so ~170 ``drop_*``
    resources are still present.
    """
    contract = _huge_spec_contract()
    spec = build_rest_spec(contract)
    names = {r.name for r in spec.resources}

    dropped_present = sorted(n for n in names if n.startswith("drop_"))
    assert not dropped_present, (
        f"low-ranked deep-nested resources survived the cap: {dropped_present[:10]}"
        f"{' ...' if len(dropped_present) > 10 else ''}. Ranking must prefer the "
        f"shallow list-capable 'keep_*' collections."
    )
    # And the survivors should all be list-capable collections (syncable).
    assert all(r.supports_list for r in spec.resources), (
        "every kept resource should be a list-capable collection"
    )


def test_truncation_emits_a_warning(caplog):
    """The cap must NOT be silent — a warning must be emitted/observable when the
    resource list is truncated.

    RED today: no warning is logged because no cap exists.
    """
    contract = _huge_spec_contract()
    with caplog.at_level(logging.WARNING):
        build_rest_spec(contract)

    warned = any(
        record.levelno >= logging.WARNING
        and ("truncat" in record.getMessage().lower()
             or "cap" in record.getMessage().lower())
        for record in caplog.records
    )
    assert warned, (
        "expected a WARNING mentioning truncation/cap when the resource list is "
        "shed from ~200 down to the cap; got warnings: "
        f"{[r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]}"
    )


def test_generated_code_only_contains_kept_resources():
    """End-to-end render recipe (mirrors the auth-contract test's in-proc style):
    build the spec, feed it to ConnectorBuilder, and assert the returned code
    string reflects the capped set — no dropped resource leaks into connector.py.

    ``include_dockerfile=False`` keeps this fully in-process (no persistence, no
    logo/network side effects).
    """
    contract = _huge_spec_contract()
    spec = build_rest_spec(contract)
    result = ConnectorBuilder().build(spec, include_dockerfile=False)

    assert result.code and result.code.strip(), (
        f"builder produced empty code (is_valid={result.is_valid}, "
        f"errors={result.validation_errors})"
    )
    # No dropped resource name should appear anywhere in the generated connector.
    assert "drop_" not in result.code, (
        "a truncated/low-ranked resource leaked into the generated connector code"
    )
