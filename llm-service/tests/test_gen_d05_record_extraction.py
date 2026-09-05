"""RED gate for d05_record_extraction — the per-resource source-dispatch method
must honour the vendor-declared ``response_data_key`` when extracting records,
not only the generic ``results`` / ``data`` / ``items`` wrapper chain.

Root cause (templates/connector.py.j2, api_saas per-resource source op):

    data = result.get('data', {})
    records = data.get('results') or data.get('data') or data.get('items') or []

For a resource whose records live under a custom wrapper key (Slack channels /
members / messages, etc.) this extracts 0 rows even though the vendor spec
carries ``response_data_key`` and it is already emitted into
``_get_resource_config`` / ``discover_schema``. The column-inference helper
``_extract_first_row`` already consults ``response_data_key`` first; the
dispatch path never did, so discovery and extraction disagree.

Render recipe is fully in-process — NO network, NO persistence to
shared/mcp-connectors, NO docker. We build a minimal GenerationContract with a
single vendor resource whose NAME (``conversations``) is deliberately different
from its ``response_data_key`` (``channels``). The key ``channels`` therefore
appears in the generated ``conversations`` dispatch method ONLY if the template
wires the response_data_key into the extraction. Today it does not -> RED.
"""

from __future__ import annotations

import os
import re
import sys

# ---------------------------------------------------------------------------
# Bootstrap so the tool_generator package's bare imports
# (``from schemas.contract import ...`` inside architect_rest.py, ``from
# generator.builder import ...``) resolve. This mirrors the proven in-container
# recipe in src/agents/tool_generator/tests/test_oauth_flow_capture_integration.py.
# ---------------------------------------------------------------------------
_TOOLGEN = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "src",
    "agents",
    "tool_generator",
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


# Resource NAME and its response_data_key are intentionally DIFFERENT so the
# key string cannot leak into the dispatch method via the resource name,
# endpoint, or the generated method name.
_RESOURCE_NAME = "conversations"
_DATA_KEY = "channels"


def _minimal_contract() -> GenerationContract:
    """A tiny REST contract with one vendor resource that declares a custom
    response_data_key. Only base_url is strictly required by build_rest_spec;
    auth defaults to bearer. vendor_rest_resources is the highest-quality
    resource source and flows through _resource_from_dict -> ResourceConfig."""
    contract = GenerationContract(session_id="d05-red", api_name="slackish")
    contract.set_fact(
        Fact(
            dimension=Dimension.BASE_URL,
            value="https://slackish.test/api",
            confidence=1.0,
            source=FactSource.USER_PROVIDED,
        )
    )
    contract.metadata["vendor_rest_resources"] = [
        {
            "name": _RESOURCE_NAME,
            "endpoint": "/conversations.list",
            "response_data_key": _DATA_KEY,
            "pagination_type": "cursor",
        }
    ]
    return contract


def _render_connector_code() -> str:
    spec = build_rest_spec(_minimal_contract())
    built = ConnectorBuilder().build(spec, include_dockerfile=False)
    assert built.is_valid, (
        "connector build failed before we could assert on extraction: "
        f"{built.validation_errors!r}"
    )
    assert built.code and built.code.strip(), "empty generated connector body"
    return built.code


def _dispatch_method_body(code: str) -> str:
    """Slice out the generated per-resource source-dispatch method
    ``def conversations(self, ...)`` up to the next method boundary. This
    scopes the assertion to the dispatch/extraction path and excludes
    _get_resource_config / discover_schema, which emit response_data_key into
    unrelated maps."""
    m = re.search(r"\n    def %s\(self" % re.escape(_RESOURCE_NAME), code)
    assert m, (
        f"generated connector has no per-resource dispatch method "
        f"def {_RESOURCE_NAME}(self, ...); the api_saas source-op path did not render"
    )
    start = m.start()
    nxt = re.search(r"\n    def ", code[start + 1 :])
    end = (start + 1 + nxt.start()) if nxt else len(code)
    return code[start:end]


def test_dispatch_extraction_references_response_data_key():
    """The dispatch method for a resource whose response_data_key='channels'
    must reference that key when extracting records, instead of relying solely
    on the generic results/data/items chain."""
    code = _render_connector_code()
    body = _dispatch_method_body(code)

    # Sanity: today's buggy output DOES contain the generic wrapper chain here.
    assert "results" in body and "items" in body, (
        "test scope drifted: the generic wrapper chain is expected to remain as "
        "a FALLBACK in the dispatch method"
    )

    # The actual gate: the custom data key must be consulted in the dispatch
    # extraction. RED on today's template (generic chain only), GREEN once the
    # template wires response_data_key into the dispatch record extraction.
    assert _DATA_KEY in body, (
        "dispatch extraction ignores response_data_key: the generated "
        f"{_RESOURCE_NAME!r} method never references {_DATA_KEY!r}, so records "
        "under a custom wrapper key extract as 0 rows. Extraction must honour "
        "resource.response_data_key first (like _extract_first_row), falling "
        "back to the generic results/data/items chain only when it is empty.\n\n"
        f"--- dispatch method body ---\n{body}"
    )
